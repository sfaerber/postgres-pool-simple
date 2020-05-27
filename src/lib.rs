pub use postgres_types::{ToSql, FromSql};
pub use tokio_postgres::Row;

use async_trait::async_trait;
use std::sync::Arc;
use arc_swap::ArcSwap;
use tokio_postgres::{Client, Config as PostgresConfig, NoTls, Statement};
use im::OrdMap;
use std::time::{Instant, Duration};
use std::future::Future;

use std::task::{Context, Poll, Waker};
use log::{trace, debug, info, warn, error};
use std::hash::Hasher;


struct ClientLease {
    client_id: u64,
    state: Arc<ArcSwap<PoolState>>,
    client: Arc<Client>,
}


impl Drop for ClientLease {
    fn drop(&mut self) {
        self.state.rcu(|inner| {
            let mut inner = (**inner).clone();
            let con = inner.working_connections.remove(&self.client_id).unwrap();
            inner.idle_connections.insert(self.client_id, con);
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
}


impl PoolState {
    fn next_idle_connection_id(&self) -> Option<u64> {
        self.idle_connections.iter().map(|(id, _)| *id).next()
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


pub struct PoolConfig {
    postgres_config: PostgresConfig,
    min_connection_count: usize,
    _max_connection_count: usize,
}


pub struct Transaction {}


impl Pool {
    fn add_connection(pool: &Self) {
        let config = pool.config.clone();
        let state = pool.state.clone();

        tokio::spawn(async move {
            match config.postgres_config.connect(NoTls).await {
                Ok((client, connection)) => {
                    let client = Arc::new(client);

                    let inc_number: u64 =
                        state.rcu(move |inner| {
                            let client = client.clone();
                            let mut inner = (**inner).clone();

                            let inc_number = inner.connection_counter + 1;
                            inner.connection_counter = inc_number;

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
                            // TODO: what if the connection is working?
                            error!("illegal state");
                        }
                        inner
                    });
                }
                Err(err) => {
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
                            inner.waiting_lease_futures.remove(&wait_id).unwrap();
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
        for waker in state.load().waiting_lease_futures
            .iter().rev().flat_map(|(_, (wo, _))| wo).next() {
            waker.wake_by_ref();
            trace!("waked the longest waiting waker");
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

    pub fn new(postgres_config: &str) -> Result<Self, PoolError> {
        //
        let mut postgres_config = postgres_config.parse::<PostgresConfig>()
            .map_err(|err| PoolError { message: err.to_string() })?;

        postgres_config.connect_timeout(Duration::from_secs(5));

        let pool = Pool {
            state: Arc::new(ArcSwap::new(Arc::new(
                PoolState {
                    idle_connections: OrdMap::new(),
                    working_connections: OrdMap::new(),
                    waiting_lease_futures: OrdMap::new(),
                    connection_counter: 0,
                    lease_future_counter: 0,
                    lease_counter: 0,
                }))),
            config: Arc::new(
                PoolConfig {
                    postgres_config,
                    min_connection_count: 50,
                    _max_connection_count: 100,
                }),
        };

        for _ in 0..pool.config.min_connection_count {
            Self::add_connection(&pool);
        }

        Ok(pool)
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
        let hash: u64 = {
            let mut hasher = ahash::AHasher::new_with_keys(4656, 1456);
            hasher.write(sql.as_bytes());
            hasher.finish()
        };

        trace!("running query with hash {}", hash);

        let lease = self.lease_client().await?;


        let stm = lease.client.prepare(sql)
            .await
            .map_err(|err| PoolError { message: err.to_string() })?;

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
                self.pool_state.rcu(|inner| {
                    let mut inner = (**inner).clone();
                    if let Some((waker, _)) = inner.waiting_lease_futures.get_mut(&self.future_id) {
                        *waker = Some(ctx.waker().clone());
                    } else {
                        panic!("illegal state")
                    }
                    inner
                });
                Poll::Pending
            }
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