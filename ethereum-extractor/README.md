Ethereum Database Load Scripts
=============

Old method
---------

`load_schema.js` is a naive implementation that loads the DB directly in JS.



New method
----------

`queue_blocks.js` loads all the ethereum blocks into Rabbit MQ

`to_db/save_blocks.py` saves queued blocks to the postgres database in a multi-threaded fashion
