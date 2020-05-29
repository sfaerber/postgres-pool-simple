pub use postgres_types::{ToSql, FromSql};
pub use tokio_postgres::{Row, Error as TokioError};

use async_trait::async_trait;
use std::sync::Arc;
use arc_swap::ArcSwap;
use tokio_postgres::{Client, Config as PostgresConfig, NoTls, Statement};
use im::OrdMap;
use std::time::{Instant, Duration};
use std::future::Future;

use std::task::{Context, Poll, Waker};
use log::{trace, debug, error, warn};
use std::hash::Hasher;
use async_std::task;


mod socket;
mod connect;


struct ClientLease {
    client_id: u64,
    state: Arc<ArcSwap<PoolState>>,
    client: Arc<Client>,
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
struct PoolState {
    idle_connections: OrdMap<u64, PooledConnection>,
    working_connections: OrdMap<u64, PooledConnection>,
    waiting_lease_futures: OrdMap<u64, (Option<Waker>, Instant)>,
    connection_counter: u64,
    lease_future_counter: u64,
    lease_counter: u64,
    currently_connecting_count: usize,
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


impl ToString for PoolError {
    fn to_string(&self) -> String {
        self.message.clone()
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
}


impl PoolConfig {
    pub fn from_config(postgres_config: PostgresConfig) -> Self {
        PoolConfig {
            postgres_config,
            connection_count: 10,
            house_keeper_interval: Duration::from_millis(250),
            lease_timeout: Duration::from_secs(10),
            max_queue_size: 25_000,
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
}


pub struct Transaction {}


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

    fn try_lease(state: &Arc<ArcSwap<PoolState>>, wait_id: Option<u64>) -> Option<ClientLease> {
        trace!("trying to lease a client for wait_id {:?}", wait_id);
        if state.load().next_idle_connection_id().is_some() {
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
                Some(ClientLease {
                    state: state.clone(),
                    client: last_state.idle_connections[&client_id].client.clone(),
                    client_id,
                })
            } else {
                None
            }
        } else {
            None
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
        if let Some(lease) = Self::try_lease(&self.state, None) {
            return Ok(lease);
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
                    connection_counter: 0,
                    lease_future_counter: 0,
                    lease_counter: 0,
                    currently_connecting_count: 0,
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


#[async_trait]
pub trait Queryable {
    async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> QueryResult;
    //async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, FlexPgPoolError>;
}


impl Pool {
    pub async fn transaction() -> Transaction {
        unimplemented!()
    }
}


#[async_trait]
impl Queryable for Pool {
    async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> QueryResult {
        //
        let statement_hash: u64 = {
            let mut hasher = ahash::AHasher::new_with_keys(4656, 1456);
            hasher.write(sql.as_bytes());
            hasher.finish()
        };

        let lease = self.lease_client().await?;

        let stm = {
            match self.state.load().working_connections
                .get(&lease.client_id).iter()
                .flat_map(|con| con.prepared_statements.get(&statement_hash)).next() {
                Some(stm) => {
                    trace!("reusing statement with hash {}", statement_hash);
                    stm.clone()
                }
                None => {
                    trace!("building statement with hash {}", statement_hash);

                    let stm = lease.client.prepare(sql)
                        .await
                        .map_err(|err| PoolError { message: err.to_string() })?;

                    self.state.rcu(|inner| {
                        let mut inner = (**inner).clone();
                        if let Some(cl) = inner.working_connections.get_mut(&lease.client_id) {
                            (*cl).prepared_statements.insert(statement_hash, stm.clone());
                        }
                        inner
                    });

                    stm
                }
            }
        };

        let rows = lease.client.query(&stm, params)
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;

        Ok(rows)
    }
}


struct ClientLeaseFuture {
    future_id: u64,
    pool_state: Arc<ArcSwap<PoolState>>,
}


impl Future for ClientLeaseFuture {
    type Output = Result<ClientLease, PoolError>;

    fn poll(self: std::pin::Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pool::try_lease(&self.pool_state, Some(self.future_id)) {
            Some(lease) => Poll::Ready(Ok(lease)),
            None => {
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


impl Transaction {
    async fn commit() -> Result<(), PoolError> {
        unimplemented!()
    }
}


#[async_trait]
impl Queryable for Transaction {
    async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> QueryResult {
        unimplemented!()
    }
}