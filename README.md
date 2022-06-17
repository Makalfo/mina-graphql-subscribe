# Mina GraphQL Subscribe
Mina GraphQL Subscription script for receiving new blocks from the Mina daemon. 
Upon receipt of a new block, the block is added to the postgres database and the best chain is 
queried for the canonical and orphaned blocks.

####
The Mina GraphQL Subscribe is currently utilized for the Mina Twitter bot (@mina_alerts) and Mina Telegram Alert bot.

See more information here:
https://makalfo.medium.com/development-of-a-mina-telegram-bot-for-mina-protocol-11cb2cceeefc
