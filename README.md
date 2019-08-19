# iACRDBSM
The real name of this project has been rebranded to sqlHeavy for extremely technical reasons that no one really undestands.

sqlHeavy is a simple relational database system written in Go for the Dropbox intern hackathon.

sqlHeavy was built in under 5 days.

sqlHeavy supports a modest subset of sql instructions including both queries and transactions.

sqlHeavy syntax is almost the same as regular sql, with a few weird (we like to think of them as fun) quirks.

sqlHeavy supports multiple client transactions and queries over the network via reader-writer locks.

sqlHeavy works by:
  1.) recieving client command strings over network via TCP
  2.) parse query/transaction command strings into an AST
  3.) generate bytecode from this AST
  4.) execute the bytecode via a virtual machine, which interacts with our simple table structured datastore to store and retrieve data.
  
sqlHeavy was built by Carolina Ortega, Linda Gong, Charles Comiter, and Sanjit Kalapatapu.

