Redis connector for husky 


The RedisInputFormat is constructed from inputformat_stroe and three parameters need to be set:
1. std::string hostname	       => arbitrary hostname of redis nodes in one cluster. 
2. int port		=>	The port used by redis
3. std::vector<std::string> keys => Redis keys should be read. Noted that only string type is supported now.

Therefore in addition to husky configuration, the config shown as below:
--redishostname <hostname> --redisport <port_number> --keys <files_containing_keys>

