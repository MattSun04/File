

#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <list>
#include <map>
#include "redis_assigner.hpp"
#include "core/constants.hpp"
#include "core/zmq_helpers.hpp"
#include "master/master.hpp"
#include "hiredis/hiredis.h"

//TODO: something you will never do
namespace husky {

	
	static RedisSplitAssigner Redis_split_assigner;

	RedisSplitAssigner::RedisSplitAssigner() : end_count_(0), split_num_(0) {
		Master::get_instance().register_main_handler(TYPE_REDIS_REQ,
			std::bind(&RedisSplitAssigner::master_redis_req_handler, this));
		Master::get_instance().register_main_handler(TYPE_REDIS_END_REQ,
			std::bind(&RedisSplitAssigner::master_redis_req_end_handler, this));
		
	}

	void RedisSplitAssigner::master_redis_req_handler() {
		auto& master = Master::get_instance();
		auto master_socket = master.get_socket();
		std::string hostname, keys;
		int port;
		BinStream stream = zmq_recv_binstream(master_socket.get());
		
		stream >>hostname>>port>>keys;
		
		RedisSplit ret = answer(hostname, port, keys);
		stream.clear();

		stream << ret;
		zmq_sendmore_string(master_socket.get(), master.get_cur_client());
		zmq_sendmore_dummy(master_socket.get());
		zmq_send_binstream(master_socket.get(), stream);
		
	}
	
	void RedisSplitAssigner::master_redis_req_end_handler() {
		auto& master = Master::get_instance();
		auto master_socket = master.get_socket();
		BinStream stream = zmq_recv_binstream(master_socket.get());
		RedisSplit split;
		stream >> split;
		recieve_end(split);

		stream.clear();
		zmq_sendmore_string(master_socket.get(), master.get_cur_client());
		zmq_sendmore_dummy(master_socket.get());
		zmq_send_binstream(master_socket.get(), stream);
		
	}


	RedisSplitAssigner::~RedisSplitAssigner() {
		available_keys_.clear();
		keys_.clear();
		splits_end_.clear();
	}
	
	
	
	
	void RedisSplitAssigner::create_splits(const std::string& hostname, int port, const std::string& key) {

		redisContext *c;
		redisReply *reply;

		//int port = 6379;
		c = redisConnect(hostname.c_str(), port);
		reply = (redisReply*)(redisCommand(c, "CLUSTER KEYSLOT %s", key.c_str() ));
		int slot = int(reply->integer);
		std::string s=std::to_string(slot);
		base::log_msg("slot: "+s);
		for (std::list<std::pair<std::pair<std::string, int>, std::pair<int, int>> > ::iterator it = slotmap_.begin(); 
			it != slotmap_.end(); ++it)
		{
			if (slot >= (*it).second.first && slot <= (*it).second.second) 
			{
				std::string host1 = (*it).first.first;
				
				std::string host2;
				for (std::list<std::pair<std::string, std::string>>::iterator it2 = master_slave_map_.begin();
						it2 != master_slave_map_.end(); ++it2)
				{
					if ((*it2).second.compare(host1) == 0)
					{
						host2 = (*it2).first;
						break;
					}
						
				}
				if (worker_resi_.find(host1) != worker_resi_.end()) {
						avaiable_keys_.insert(std::pair<std::string, std::string>(key, host1));
					}
				avaiable_keys_.insert(std::pair<std::string, std::string>(key, host2));
				break;
			}
		}
		
		freeReplyObject(reply);
		redisFree(c);
	}
	
	void RedisSplitAssigner::initialize_map(const std::string& hostname,int port) {

		redisContext *c;
		redisReply *reply;

		//int port = 6379;
		c = redisConnect(hostname.c_str(), port);
		reply = (redisReply*)(redisCommand(c, "CLUSTER NODES"));
		printf("result : ", reply->str);
		std::string s(reply->str);

		boost::char_separator<char> sep("\n");
		boost::tokenizer<boost::char_separator<char>> tok(s, sep);
	
		std::list<std::pair<std::string, std::string>> master;
		std::list<std::pair<std::string, std::string>> slave;
		for (auto& w : tok) {
			int count = 0;
			boost::char_separator<char> sep2(" ,");
			boost::tokenizer<boost::char_separator<char> > tok2(w, sep2);
			std::string host, id;
			int port, type, min, max;   //1 for master, 0 for slave
			std::string last_string;
			for (auto& c : tok2)
			{
				count++;
				if (count == 1)
					id = c;
				else if (count == 2)
					host = c;
				else if (count == 3)
					port = std::stoi(c);
				else if (count == 4)
				{
					if (c.compare("master") == 0)
						type = 1;
					else if (c.compare("slave") == 0)
						type = 0;
					else
						count--;
				}
				else if (count == 5)
					if (type == 0)
						id = c;
					else
						last_string = c;
			}
			std::pair<std::string, std::string> id_pair = { id,host };
			if (type == 1)
			{
				boost::char_separator<char> sep3("-");
				boost::tokenizer<boost::char_separator<char> > tok3(last_string, sep3);
				int count_min = 0;
				for (auto& c : tok3)
				{
					count_min++;
					if (count_min == 1)
						min = std::stoi(c);
					else
						max = std::stoi(c);
				}
				std::pair<int, int> slot = { min,max };
				std::pair<std::string, int> hostmap = { host, port };
				worker_resi_.insert(host);
				std::pair<std::pair<std::string, int>, std::pair<int, int> > slotmap = { hostmap, slot };
				slotmap_.push_back(slotmap);
				master.push_back(id_pair);
			}
			else
				slave.push_back(id_pair);
		}
		for (std::list<std::pair<std::string,std::string> >::iterator it = slave.begin(); it != slave.end(); ++it) 
		{
			std::string s=it->first;
			for (std::list<std::pair<std::string, std::string> >::iterator it2 = master.begin(); it2 != master.end(); ++it2)
			{
				if (s.compare(it2->first)==0) 
				{
					std::pair<std::string, std::string> p = { it->second,it2->second };
					master_slave_map_.push_back(p);
				}
			}
		}
		for (std::set<std::string>::iterator it = worker_.begin(); it != worker_.end(); ++it)
		{
			if (worker_resi_.find(*it) != worker_resi_.end()) 
			{
				worker_resi_.erase(worker_resi_.find(*it));
			}
		
		}


		freeReplyObject(reply);
		redisFree(c);
	}
	




	RedisSplit RedisSplitAssigner::answer(const std::string& hostname, int port, const std::string& key) {

		if (hostname_.compare(hostname) != 0)
		{
			initialize_map(hostname, port);		//collecting information from redis cluster
			if (std::find(key_.begin(), key_.end(), key) != key_.end())
			{
				create_splits(hostname,port,key);
				key_.push_back(key);
			}
		}
		
		auto& master = Master::get_instance();
		std::string client = master.get_cur_client();

		std::string n;
		boost::char_separator<char> sep(".");
		boost::tokenizer<boost::char_separator<char>> tok(client, sep);
		for (auto& c : tok) 
		{
			n=std::to_string(c.back()-48);
			break;
		}
		client = "192.168.50."+ n;			//hard core here as encountering some problems map hostname to ip
		std::string key_read;
		std::string address;
		for(std::set<std::pair<std::string,std::string>>::iterator it = available_keys_.begin(); it != available_keys_.end(); ++it) 
		{
			if (client.compare((*it).second) == 0) 
			{
				key_read = (*it).first;
				break;
			}
		}
		if (key_read.empty()) 
		{
			RedisSplit ret = RedisSplit();
			ret.set_valid(false);
			return ret;		
		}
		else 
		{
			RedisSplit ret = RedisSplit();
			ret.set_port(port_);
			
			for (std::set<std::pair<std::string, std::string>>::iterator it2 = available_keys_.begin(); it2 != available_keys_.end(); ++it2)
			{
				if (key_read.compare((*it2).first) == 0)
				{
					available_keys_.erase(it2);
				}
			}
			for (std::list<std::pair<std::string, std::string> >::iterator it3 = master_slave_map_.begin(); it3 != master_slave_map_.end(); ++it3)
			{
				if (client.compare(it3->first) == 0)
				{
					client = it3->second;
					break;
				}
			}
			ret.set_hostname(client);
			ret.set_key(key_read);
			return ret;
		}
		
		
		
	}
	
	void RedisSplitAssigner::recieve_end(RedisSplit& split) {
		if (!split.is_valid())
			return;

		end_count_++;
		splits_end_.push_back(split);
	}
	
}  // namespace husky


