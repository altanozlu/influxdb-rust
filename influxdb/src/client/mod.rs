//! Client which can read and write data from InfluxDB.
//!
//! # Arguments
//!
//!  * `url`: The URL where InfluxDB is running (ex. `http://localhost:8086`).
//!  * `database`: The Database against which queries and writes will be run.
//!
//! # Examples
//!
//! ```rust
//! use influxdb::Client;
//!
//! let client = Client::new("http://localhost:8086", "test");
//!
//! assert_eq!(client.database_name(), "test");
//! ```

use futures::prelude::*;

use crate::query::QueryType;
use crate::Error;
use crate::Query;
use std::sync::Arc;

#[derive(Clone, Debug)]
/// Internal Representation of a Client
pub struct Client {
    pub(crate) url: Arc<String>,
    pub(crate) database: (&'static str, String),
    pub(crate) credentials: Option<([(&'static str, String); 2])>,
    pub(crate) client: reqwest::Client,
}

impl Client {
    /// Instantiates a new [`Client`](crate::Client)
    ///
    /// # Arguments
    ///
    ///  * `url`: The URL where InfluxDB is running (ex. `http://localhost:8086`).
    ///  * `database`: The Database against which queries and writes will be run.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use influxdb::Client;
    ///
    /// let _client = Client::new("http://localhost:8086", "test");
    /// ```
    pub fn new<S1, S2>(url: S1, database: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Client {
            url: Arc::new(url.into()),
            client: reqwest::Client::new(),
            database: ("db", database.into()),
            credentials: None,
        }
    }

    /// Add authentication/authorization information to [`Client`](crate::Client)
    ///
    /// # Arguments
    ///
    /// * username: The Username for InfluxDB.
    /// * password: The Password for the user.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use influxdb::Client;
    ///
    /// let _client = Client::new("http://localhost:9086", "test").with_auth("admin", "password");
    /// ```
    pub fn with_auth<S1, S2>(mut self, username: S1, password: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        self.credentials = Some([("u", username.into()), ("p", password.into())]);
        self
    }

    /// Returns the name of the database the client is using
    pub fn database_name(&self) -> &str {
        // safe to unwrap: we always set the database name in `Self::new`
        &self.database.1
    }

    /// Returns the URL of the InfluxDB installation the client is using
    pub fn database_url(&self) -> &str {
        &self.url
    }

    /// Pings the InfluxDB Server
    ///
    /// Returns a tuple of build type and version number
    pub async fn ping(&self) -> Result<(String, String), Error> {
        let url = &format!("{}/ping", self.url);
        let request = self
            .client
            .request(reqwest::Method::GET, url)
            .build()
            .map_err(|err| Error::UrlConstructionError {
                error: err.to_string(),
            })?;
        let res = self
            .client
            .execute(request)
            .await
            .map_err(|err| Error::ProtocolError {
                error: format!("{}", err),
            })?;

        let build = res
            .headers()
            .get("X-Influxdb-Build")
            .unwrap()
            .to_str()
            .unwrap();
        let version = res
            .headers()
            .get("X-Influxdb-Version")
            .unwrap()
            .to_str()
            .unwrap();

        Ok((build.to_owned(), version.to_owned()))
    }

    /// Sends a [`ReadQuery`](crate::ReadQuery) or [`WriteQuery`](crate::WriteQuery) to the InfluxDB Server.
    ///
    /// A version capable of parsing the returned string is available under the [serde_integration](crate::integrations::serde_integration)
    ///
    /// # Arguments
    ///
    ///  * `q`: Query of type [`ReadQuery`](crate::ReadQuery) or [`WriteQuery`](crate::WriteQuery)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use influxdb::{Client, Query, Timestamp};
    /// use influxdb::InfluxDbWriteable;
    /// use std::time::{SystemTime, UNIX_EPOCH};
    ///
    /// # #[async_std::main]
    /// # async fn main() -> Result<(), influxdb::Error> {
    /// let start = SystemTime::now();
    /// let since_the_epoch = start
    ///   .duration_since(UNIX_EPOCH)
    ///   .expect("Time went backwards")
    ///   .as_millis();
    ///
    /// let client = Client::new("http://localhost:8086", "test");
    /// let query = Timestamp::Milliseconds(since_the_epoch)
    ///     .into_query("weather")
    ///     .add_field("temperature", 82);
    /// let results = client.query(&query).await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    /// # Errors
    ///
    /// If the function can not finish the query,
    /// a [`Error`] variant will be returned.
    ///
    /// [`Error`]: enum.Error.html
    pub async fn query<'q, Q>(&self, q: &'q Q) -> Result<String, Error>
    where
        Q: Query,
    {
        let query = q.build().map_err(|err| Error::InvalidQueryError {
            error: err.to_string(),
        })?;

        let request = match q.get_type() {
            QueryType::ReadQuery => {
                let read_query = query.get();
                let url = &format!("{}/query", &self.url);

                if read_query.contains("SELECT") || read_query.contains("SHOW") {
                    self.build_db_request(reqwest::Method::GET, url)
                        .query(&[("q", read_query)])
                        .build()
                } else {
                    self.build_db_request(reqwest::Method::POST, url)
                        .query(&[("q", read_query)])
                        .build()
                }
            }
            QueryType::WriteQuery(precision) => {
                let url = &format!("{}/write", &self.url);
                self.build_db_request(reqwest::Method::POST, url)
                    .body(query.get())
                    .query(&[("precision", precision)])
                    .build()
            }
        }
        .map_err(|err| Error::UrlConstructionError {
            error: err.to_string(),
        })?;

        let request_exec = self.client.execute(request).await;
        let response = match request_exec {
            Ok(res) => res,
            Err(err) => {
                return Err(Error::ConnectionError {
                    error: err.to_string(),
                });
            }
        };
        match response.status() {
            reqwest::StatusCode::UNAUTHORIZED => return Err(Error::AuthorizationError),
            reqwest::StatusCode::FORBIDDEN => return Err(Error::AuthenticationError),
            _ => {}
        }
        let s = response
            .text()
            .await
            .map_err(|_| Error::DeserializationError {
                error: "response could not be converted to UTF-8".to_string(),
            })?;

        // todo: improve error parsing without serde
        if s.contains("\"error\"") {
            return Err(Error::DatabaseError {
                error: format!("influxdb error: \"{}\"", s),
            });
        }

        Ok(s)
    }

    pub fn build_db_request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.request(method, url).query(&[&self.database]);

        if let Some(credentials) = &self.credentials {
            req = req.query(&credentials);
        }

        req
    }
}

#[cfg(test)]
mod tests {
    use super::Client;

    #[test]
    fn test_fn_database() {
        let client = Client::new("http://localhost:8068", "database");
        assert_eq!(client.database_name(), "database");
        assert_eq!(client.database_url(), "http://localhost:8068");
    }

    #[test]
    fn test_with_auth() {
        let client = Client::new("http://localhost:8068", "database");
        assert_eq!(client.database, ("db", "database".to_string()));

        let with_auth = client.with_auth("username", "password");
        assert_eq!(
            with_auth.credentials,
            Some([("u", "username".to_string()), ("p", "password".to_string()),])
        );
    }
}
