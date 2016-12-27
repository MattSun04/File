#include <string>

#include "io/input/redis_inputformat.hpp"

#include <string>
#include <vector>


#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"
#include "hiredis.h"

namespace husky {
	namespace io {

		bool RedisInputFormat::is_setup() const 
		{ 
			return true; 
		}
		
		RedisInputFormat::RedisInputFormat() {
			
			records_vector_.clear();
		}

		RedisInputFormat::~RedisInputFormat() 
		{ 
			records_vector_.clear(); 
		}


		void RedisInputFormat::set_hostname(const std::string& hostname) {
			hostname_ = hostname;
		}


		void RedisInputFormat::set_port(int port) {
			port_ = port;
		}
		void RedisInputFormat::set_keys(std::vector<std::string> v) {
			keys_ = v;
		}


		void RedisInputFormat::ask_split() {
			BinStream question;
			
			question << hostname_ << port_  << keys_; // husky::Context::get_param("hostname")
			BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_REQ);
			answer >> split_;
			return;
		}

		void RedisInputFormat::read() {
			if (!records_vector_.empty())
				records_vector_.clear();
			redisContext *c;
			redisReply *reply;
			reply->type;
			const char *hostname = split_.get_hostname().c_str();
			int port = 6379;
			c = redisConnect(hostname, port);
			reply = (redisReply*)(redisCommand(c, "GET %s",split_.get_key()));
			printf("result : ", reply->str);
			std::string s(reply->str);
			records_vector_.push_back(s);
			freeReplyObject(reply);
			redisFree(c);
			return;
		}

		void RedisInputFormat::send_end() {
			BinStream question;
			question << split_;
			husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_END_REQ);
			return;
		}

		bool RedisInputFormat::next(RecordT& ref) {
			while (records_vector_.empty()) {
				ask_split();
				if (!split_.is_valid())
					return false;
				read();
				if (records_vector_.empty())
					send_end();
			}

			ref = records_vector_.back();
			records_vector_.pop_back();
			if (records_vector_.empty())
				send_end();
			return true;
		}

	}  // namespace io
}  // namespace husky
