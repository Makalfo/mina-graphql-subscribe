# Mina GraphQL Query
Mina GraphQL Subscription script for receiving new blocks from the Mina daemon. 
Upon receipt of a new block, the block is added to the postgres database and the best chain is 
queried for the canonical and orphaned blocks.
