pub use postgres_types::{ToSql, FromSql};
pub use tokio_postgres::{Row, Error as TokioError};

use std::sync::Arc;
use arc_swap::ArcSwap;
use tokio_postgres::{Client, Config as PostgresConfig, NoTls, Statement};
use im::OrdMap;
use std::time::{Instant, Duration};
use std::future::Future;

use std::task::{Context, Poll, Waker};
use log::{trace, debug, error, warn};
use std::{pin::Pin, hash::Hasher};
use async_std::task;

mod socket;
mod connect;


struct ClientLease {
    client_id: u64,
    state: Arc<ArcSwap<PoolState>>,
    client: Arc<Client>,
}


impl ClientLease {
    async fn get_or_create_statement(&self, sql: &str) -> Result<Statement, PoolError> {
        let statement_hash: u64 = {
            let mut hasher = ahash::AHasher::new_with_keys(4656, 1456);
            hasher.write(sql.as_bytes());
            hasher.finish()
        };

        match self.state.load().working_connections
            .get(&self.client_id).iter()
            .flat_map(|con| con.prepared_statements.get(&statement_hash)).next() {
            Some(stm) => {
                trace!("reusing statement with hash {}", statement_hash);
                Ok(stm.clone())
            }
            None => {
                trace!("building statement with hash {}", statement_hash);

                let stm = self.client.prepare(sql)
                    .await
                    .map_err(|err| PoolError { message: err.to_string() })?;

                self.state.rcu(|inner| {
                    let mut inner = (**inner).clone();
                    if let Some(cl) = inner.working_connections.get_mut(&self.client_id) {
                        (*cl).prepared_statements.insert(statement_hash, stm.clone());
                    }
                    inner
                });

                Ok(stm)
            }
        }
    }
}


impl Drop for ClientLease {
    fn drop(&mut self) {
        self.state.rcu(|inner| {
            let mut inner = (**inner).clone();
            // connection may be remove while being leased
            if let Some(con) = inner.working_connections.remove(&self.client_id) {
                inner.idle_connections.insert(self.client_id, con);
            }
            inner
        });

        Pool::try_wake_waiting(&self.state);
        trace!("lease {} dropped", self.client_id)
    }
}


#[derive(Clone)]
struct PooledConnection {
    client: Arc<Client>,
    opened: Instant,
    inc_number: u64,
    prepared_statements: OrdMap<u64, Statement>,
}


#[derive(Clone)]
struct TransactionData {
    created: Instant,
    client_id: u64,
}


#[derive(Clone)]
struct PoolState {
    idle_connections: OrdMap<u64, PooledConnection>,
    working_connections: OrdMap<u64, PooledConnection>,
    waiting_lease_futures: OrdMap<u64, (Option<Waker>, Instant)>,
    transactions: OrdMap<u64, TransactionData>,
    connection_counter: u64,
    lease_future_counter: u64,
    lease_counter: u64,
    currently_connecting_count: usize,
    transactions_counter: u64,
}


impl PoolState {
    fn next_idle_connection_id(&self) -> Option<u64> {
        self.idle_connections.iter().map(|(id, _)| *id).next()
    }
    fn overall_connection_count(&self) -> usize {
        self.currently_connecting_count +
            self.idle_connections.len() +
            self.working_connections.len()
    }
    fn longest_waiting_lease_waker(&self) -> Option<u64> {
        self.waiting_lease_futures
            .iter()
            .rev()
            .filter(|(_, (wo, _))| wo.is_some())
            .map(|(id, _)| *id)
            .next()
    }
    fn timed_out_leases(&self, timed_out_when_started_before: Instant) -> Vec<u64> {
        self.waiting_lease_futures
            .iter()
            .filter(|(_, (_, t))| t < &timed_out_when_started_before)
            .map(|(id, _)| *id)
            .collect()
    }
}


#[derive(Debug)]
pub struct PoolError {
    message: String,
}


impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}


#[derive(Clone)]
pub struct Pool {
    state: Arc<ArcSwap<PoolState>>,
    config: Arc<PoolConfig>,
}


#[derive(Clone)]
pub struct PoolConfig {
    postgres_config: PostgresConfig,
    connection_count: usize,
    house_keeper_interval: Duration,
    lease_timeout: Duration,
    max_queue_size: usize,
    execute_on_connect: String,
}


impl PoolConfig {
    pub fn from_config(postgres_config: PostgresConfig) -> Self {
        PoolConfig {
            postgres_config,
            connection_count: 10,
            house_keeper_interval: Duration::from_millis(250),
            lease_timeout: Duration::from_secs(10),
            max_queue_size: 5_000,
            execute_on_connect: "".to_string(),
        }
    }
    pub fn from_connection_string(postgres_config: &str) -> Result<Self, TokioError> {
        Ok(Self::from_config(postgres_config.parse::<PostgresConfig>()?))
    }
    pub fn start_pool(&self) -> Result<Pool, PoolError> {
        let mut cfg = self.clone();

        if cfg.postgres_config.get_connect_timeout().is_none() {
            cfg.postgres_config.connect_timeout(cfg.lease_timeout);
            warn!(
                "'connect_timeout' in postgres was None, 'lease_timeout' of {} sec will be used",
                cfg.lease_timeout.as_secs_f32(),
            );
        }

        if cfg.connection_count == 0 || cfg.connection_count > 1000 {
            Err(PoolError {
                message: format!(
                    "'connection_count' must be between 1 and 1000, got {}",
                    cfg.connection_count,
                )
            })
        } else if cfg.house_keeper_interval.as_millis() < 10 ||
            cfg.house_keeper_interval.as_millis() > 10_000 {
            Err(PoolError {
                message: format!(
                    "'house_keeper_interval' must be between 10 and 10_000 ms, got {} ms",
                    cfg.house_keeper_interval.as_millis(),
                )
            })
        } else if cfg.max_queue_size == 0 || cfg.max_queue_size > 25_000 {
            Err(PoolError {
                message: format!(
                    "'max_queue_size' must be between 1 and 25_000, got {}",
                    cfg.max_queue_size
                )
            })
        } else if cfg.lease_timeout <= (cfg.house_keeper_interval * 2) {
            Err(PoolError {
                message: format!(
                    "{}, got {}ms <= ({}ms * 2)",
                    "required 'lease_timeout' > ('house_keeper_interval' * 2)",
                    cfg.lease_timeout.as_millis(),
                    cfg.house_keeper_interval.as_millis(),
                )
            })
        } else {
            Ok(Pool::start(cfg))
        }
    }

    pub fn connection_count(&mut self, connection_count: usize) -> &mut Self {
        self.connection_count = connection_count;
        self
    }

    pub fn house_keeper_interval(&mut self, house_keeper_interval: Duration) -> &mut Self {
        self.house_keeper_interval = house_keeper_interval;
        self
    }

    pub fn lease_timeout(&mut self, lease_timeout: Duration) -> &mut Self {
        self.lease_timeout = lease_timeout;
        self
    }

    pub fn max_queue_size(&mut self, max_queue_size: usize) -> &mut Self {
        self.max_queue_size = max_queue_size;
        self
    }

    pub fn execute_on_connect(&mut self, execute_on_connect: &str) -> &mut Self {
        self.execute_on_connect += execute_on_connect;
        self.execute_on_connect += "; ";
        self
    }
}


impl Pool {
    fn add_connection(pool: &Self) {
        let config = pool.config.clone();
        let state = pool.state.clone();

        task::spawn(async move {
            state.rcu(move |inner| {
                let mut inner = (**inner).clone();
                inner.currently_connecting_count += 1;
                inner
            });
            match connect::connect_tls(config.postgres_config.clone(), NoTls).await {
                Ok((client, connection)) => {
                    let client = Arc::new(client);

                    if config.execute_on_connect.len() > 0 {
                        let client = client.clone();
                        task::spawn(async move {
                            if let Err(err) = client.batch_execute(
                                &config.execute_on_connect).await {
                                error!(
                                    "could execute '{}' configured to run on connection create: {}",
                                    config.execute_on_connect,
                                    err
                                );
                            }
                        });
                    }

                    let inc_number: u64 =
                        state.rcu(move |inner| {
                            let client = client.clone();
                            let mut inner = (**inner).clone();

                            let inc_number = inner.connection_counter + 1;
                            inner.connection_counter = inc_number;
                            inner.currently_connecting_count -= 1;

                            inner.idle_connections.insert(
                                inc_number,
                                PooledConnection {
                                    client,
                                    opened: Instant::now(),
                                    inc_number,
                                    prepared_statements: OrdMap::new(),
                                },
                            );
                            inner
                        }).connection_counter + 1;

                    Pool::try_wake_waiting(&state);

                    debug!("connection number {} created", inc_number);

                    let result = connection.await;

                    debug!("connection terminated with result '{:?}'", result);

                    state.rcu(move |inner| {
                        let mut inner = (**inner).clone();
                        if inner.idle_connections.remove(&inc_number).is_none() {
                            if inner.working_connections.remove(&inc_number).is_none() {
                                error!(
                                    "could not find client for terminated connection {}",
                                    inc_number
                                );
                            }
                        }
                        inner
                    });
                }
                Err(err) => {
                    state.rcu(move |inner| {
                        let mut inner = (**inner).clone();
                        inner.currently_connecting_count -= 1;
                        inner
                    });
                    error!("could not connect: {}, ", err)
                }
            }
        });
    }

    fn try_lease(
        state: &Arc<ArcSwap<PoolState>>,
        max_queue_size: usize,
        wait_id: Option<u64>,
    ) -> Result<Option<ClientLease>, PoolError> {
        let (has_idle, queue_full) = {
            let state = state.load();
            (
                state.next_idle_connection_id().is_some(),
                state.waiting_lease_futures.len() > max_queue_size
            )
        };

        if queue_full {
            return Err(PoolError {
                message: format!(
                    "could not get connection: wait queue with limit {} is full",
                    max_queue_size
                )
            });
        }

        trace!("trying to lease a client for wait_id {:?}", wait_id);
        if has_idle {
            let last_state =
                state.rcu(|inner| {
                    let mut inner = (**inner).clone();
                    if let Some(id) = inner.next_idle_connection_id() {
                        let con = inner.idle_connections.remove(&id).unwrap();
                        inner.working_connections.insert(id, con);
                        inner.lease_counter += 1;
                        if let Some(wait_id) = wait_id {
                            inner.waiting_lease_futures.remove(&wait_id);
                        }
                    }
                    inner
                });

            if let Some(client_id) = last_state.next_idle_connection_id() {
                trace!("leased a client for wait_id {:?}", wait_id);
                Ok(Some(ClientLease {
                    state: state.clone(),
                    client: last_state.idle_connections[&client_id].client.clone(),
                    client_id,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn try_wake_waiting(state: &Arc<ArcSwap<PoolState>>) {
        trace!("waking lease futures, count={}", state.load().waiting_lease_futures.len());

        if state.load().longest_waiting_lease_waker().is_none() {
            return;
        }

        let last_state =
            state.rcu(|s| {
                let mut inner = (**s).clone();
                if let Some(id) = inner.longest_waiting_lease_waker() {
                    inner.waiting_lease_futures[&id].0 = None;
                }
                inner
            });

        if let Some(id) = last_state.longest_waiting_lease_waker() {
            last_state.waiting_lease_futures[&id].0.as_ref().unwrap().wake_by_ref();
            trace!("waked the longest waiting waker {}", id);
        }
    }

    async fn lease_client(&self) -> Result<ClientLease, PoolError> {
        match Self::try_lease(&self.state, self.config.max_queue_size, None) {
            Ok(Some(lease)) => return Ok(lease),
            Err(err) => return Err(err),
            _ => (),
        }

        let now = Instant::now();

        let future_id =
            self.state.rcu(|inner| {
                let mut inner = (**inner).clone();
                let future_id = inner.lease_future_counter + 1;
                inner.lease_future_counter = future_id;
                inner.waiting_lease_futures.insert(future_id, (None, now));
                inner
            }).lease_future_counter + 1;

        ClientLeaseFuture {
            future_id,
            pool_state: self.state.clone(),
            max_queue_size: self.config.max_queue_size,
        }.await
    }

    fn start(config: PoolConfig) -> Self {
        //
        let pool = Pool {
            state: Arc::new(ArcSwap::new(Arc::new(
                PoolState {
                    idle_connections: OrdMap::new(),
                    working_connections: OrdMap::new(),
                    waiting_lease_futures: OrdMap::new(),
                    transactions: OrdMap::new(),
                    connection_counter: 0,
                    lease_future_counter: 0,
                    lease_counter: 0,
                    currently_connecting_count: 0,
                    transactions_counter: 0,
                }))),
            config: Arc::new(config),
        };

        for _ in 0..pool.config.connection_count {
            Self::add_connection(&pool);
        }

        let cloned_pool = pool.clone();

        // here we start the initial pool of connections
        task::spawn(async move {
            for _ in 0..cloned_pool.config.connection_count {
                Self::add_connection(&cloned_pool);
                task::sleep(
                    cloned_pool.config.house_keeper_interval /
                        (cloned_pool.config.connection_count as u32 + 1)
                ).await;
            }
        });

        // here we start the housekeeper task
        let cloned_pool = pool.clone();

        task::spawn(async move {
            loop {
                task::sleep(cloned_pool.config.house_keeper_interval).await;
                cloned_pool.do_house_keeping();
            }
        });

        pool
    }

    fn do_house_keeping(&self) {
        //
        let timed_out_when_started_before = Instant::now() - self.config.lease_timeout;

        let (waiting_count, connection_count, longest_waiting_lease_waker, timeout_count) = {
            let state = self.state.load();
            (
                state.waiting_lease_futures.len(),
                state.overall_connection_count(),
                state.longest_waiting_lease_waker(),
                state.waiting_lease_futures.iter()
                    .filter(|(_, (_, t))| t < &timed_out_when_started_before)
                    .count()
            )
        };

        trace!(
            "house keeper: waiting: {}, connections: {}, longest waiting id: {:?}, timed_out: {}",
            waiting_count,
            connection_count,
            longest_waiting_lease_waker,
            timeout_count
        );

        if timeout_count > 0 {
            let old_state =
                self.state.rcu(|s| {
                    let mut s = (**s).clone();
                    for id in s.timed_out_leases(timed_out_when_started_before) {
                        s.waiting_lease_futures.remove(&id);
                    }
                    s
                });

            let timed_out = old_state.timed_out_leases(timed_out_when_started_before);

            debug!("{} timed out lease futures are removed from pool", timed_out.len());

            for id in timed_out {
                if let Some(waker) = &old_state.waiting_lease_futures[&id].0 {
                    waker.wake_by_ref()
                }
            }
        }

        if connection_count < self.config.connection_count {
            debug!("building a new connection because there are to few in pool");
            Self::add_connection(&self);
        }
    }
}


pub type QueryResult = Result<Vec<Row>, PoolError>;


pub trait Queryable : Sync {

    fn query<'a>(&'a self, sql: &'a str, params: &'a [&(dyn ToSql + Sync)]) -> Pin<Box<dyn Future<Output=QueryResult> + Send + 'a>>;

    fn execute<'a>(&'a self, sql: &'a str, params: &'a [&(dyn ToSql + Sync)]) -> Pin<Box<dyn Future<Output=Result<u64, PoolError>> + Send + 'a>>;
}


impl Pool {
    pub async fn transaction(&self) -> Result<Transaction, PoolError> {
        let client = self.lease_client().await?;

        let begin = client.get_or_create_statement("BEGIN").await?;

        client.client.execute(&begin, &[]).await
            .map_err(|err| PoolError { message: err.to_string() })?;

        let created = Instant::now();

        let transaction_id =
            self.state.rcu(|inner| {
                let mut inner = (**inner).clone();
                let transaction_id = inner.transactions_counter + 1;
                inner.transactions_counter = transaction_id;
                inner.transactions.insert(transaction_id, TransactionData {
                    created,
                    client_id: client.client_id,
                });
                inner
            }).transactions_counter + 1;


        Ok(Transaction {
            client_lease: Arc::new(client),
            transaction_id,
        })
    }

    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> QueryResult {
        let lease = self.lease_client().await?;

        let stm = lease.get_or_create_statement(&sql).await?;

        let rows = lease.client.query(&stm, params)
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;

        Ok(rows)
    }

    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, PoolError> {
        let lease = self.lease_client().await?;

        let stm = lease.get_or_create_statement(&sql).await?;

        let row_count = lease.client.execute(&stm, params)
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;

        Ok(row_count)
    }
}


impl Queryable for Pool {

    fn query<'a>(&'a self, sql: &'a str, params: &'a [&(dyn ToSql + Sync)]) -> Pin<Box<dyn Future<Output=QueryResult> + Send + 'a>> {
        Box::pin( async move { 
            self.query(sql, params).await
        })
    }

    fn execute<'a>(&'a self, sql: &'a str, params: &'a [&(dyn ToSql + Sync)]) -> Pin<Box<dyn Future<Output=Result<u64, PoolError>> + Send + 'a>> {
        Box::pin( async move { 
            self.execute(sql, params).await
        })
    }
}


struct ClientLeaseFuture {
    future_id: u64,
    pool_state: Arc<ArcSwap<PoolState>>,
    max_queue_size: usize,
}


impl Future for ClientLeaseFuture {
    type Output = Result<ClientLease, PoolError>;

    fn poll(self: std::pin::Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pool::try_lease(&self.pool_state, self.max_queue_size, Some(self.future_id)) {
            Ok(Some(lease)) => Poll::Ready(Ok(lease)),
            Ok(None) => {
                if self.pool_state.rcu(|inner| {
                    let mut inner = (**inner).clone();
                    if let Some((waker, _)) = inner.waiting_lease_futures.get_mut(&self.future_id) {
                        *waker = Some(ctx.waker().clone());
                    }
                    inner
                }).waiting_lease_futures.contains_key(&self.future_id) {
                    Poll::Pending
                } else {
                    Poll::Ready(Err(PoolError {
                        message: format!(
                            "unable to allocate a database connection within 'lease_timeout'",
                        )
                    }))
                }
            }
            Err(err) => Poll::Ready(Err(err))
        }
    }
}


impl Drop for ClientLeaseFuture {
    fn drop(&mut self) {
        if self.pool_state.load().waiting_lease_futures.contains_key(&self.future_id) {
            trace!("future {} was dropped while still in wait list", self.future_id);
            self.pool_state.rcu(|s| {
                let mut s = (**s).clone();
                s.waiting_lease_futures.remove(&self.future_id);
                s
            });
        }
    }
}


pub struct Transaction {
    client_lease: Arc<ClientLease>,
    transaction_id: u64,
}


impl Transaction {
    fn done(&self) {
        trace!("removing transaction from state");
        self.client_lease.state.rcu(|s| {
            let mut s = (**s).clone();
            s.transactions.remove(&self.transaction_id).unwrap();
            s
        });
    }

    pub async fn commit(self) -> Result<(), PoolError> {
        let stm = self.client_lease.get_or_create_statement("COMMIT").await?;
        self.client_lease.client.execute(&stm, &[])
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;
        debug!("transaction committed");
        self.done();
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), PoolError> {
        let stm = self.client_lease.get_or_create_statement("ROLLBACK").await?;
        self.client_lease.client.execute(&stm, &[])
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;
        debug!("transaction manually rolled back");
        self.done();
        Ok(())
    }

    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> QueryResult {
        let stm = self.client_lease.get_or_create_statement(&sql).await?;

        let rows = self.client_lease.client.query(&stm, params)
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;

        Ok(rows)
    }

    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, PoolError> {
        let stm = self.client_lease.get_or_create_statement(&sql).await?;

        let row_count = self.client_lease.client.execute(&stm, params)
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;

        Ok(row_count)
    }
}


impl Drop for Transaction {
    fn drop(&mut self) {
        if self.client_lease.state.load().transactions.contains_key(&self.transaction_id) {
            debug!("auto rolling back undone transaction");
            self.done();
            let lease = self.client_lease.clone();
            let transaction_id = self.transaction_id;
            task::spawn(async move {
                let log_error = |err: String| {
                    error!(
                        "error while rolling back dropped transaction {}: {}",
                        transaction_id, err
                    );
                };

                match lease.get_or_create_statement("ROLLBACK").await {
                    Ok(stm) =>
                        if let Err(err) = lease.client.execute(&stm, &[]).await {
                            log_error(err.to_string());
                        } else {
                            debug!("auto rollback done");
                        },
                    Err(err) => log_error(err.to_string()),
                }
            });
        }
    }
}


impl Queryable for Transaction {

    fn query<'a>(&'a self, sql: &'a str, params: &'a [&(dyn ToSql + Sync)]) -> Pin<Box<dyn Future<Output=QueryResult> + Send + 'a>> {
        Box::pin( async move { 
            self.query(sql, params).await
        })
    }

    fn execute<'a>(&'a self, sql: &'a str, params: &'a [&(dyn ToSql + Sync)]) -> Pin<Box<dyn Future<Output=Result<u64, PoolError>> + Send + 'a>> {
        Box::pin( async move { 
            self.execute(sql, params).await
        })
    }
}
