- [ ] Bench the current implementation with redis
- [ ] Finish implemeting BLPOP 
- [ ] Improve parsing by doing it zero-allocation
- [ ] Implenet a redis client for testing


Current replication implementation consisted in a server refactor, where Connection represent a connection handle over IO-commands, while server manages commands, 
the transactional logic in the connection side and the exectute the qeueud commands in server logic.
