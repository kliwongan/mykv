impl Server {
    async fn run<F>(self, handler: F) -> Result<(), Error>
    where
        F: Fn(HttpRequest) -> HttpResponse,
    {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            let mut connection = listener.accept().await?;
            let request = timeout(read_http_request(&mut connection).await?);

            // bad timeout
            if request {
                // BECOME candidate
                // send request vote requests
                // await responses by doing continue statement
            }

            // here we check for state of current raft node
            task::spawn(async move {
                // Call the handler provided by the user
                let response = handler(request);

                write_http_response(connection, response).await?;
            });
        }
    }
}