#pragma once



#include <string>

#include <vector>


#include "/io/input/inputformat_base.hpp"
#include "/io/input/redis_split.hpp"



namespace husky {

	namespace io {
		class RedisInputFormat final : public InputFormatBase {

		public:
			//TODO: other type: List Set
			typedef std::string RecordT;
			RedisInputFormat();
			virtual ~RedisInputFormat();



			//void set_auth(const std::string& username, const std::string& password);


			void set_hostname(const std::string& hostname);
			void set_port(int port);
			void set_keys(std::vector<std::string> v);
			virtual bool is_setup() const;



			virtual bool next(RecordT& ref);

			void ask_split();
			void read();
			void send_end();



		protected:

			bool need_auth_ = false;

			RedisSplit split_;

			std::string hostname_;
			int port_;
			std::vector<std::string> keys_;
			std::vector<RecordT> records_vector_;

		};



	}  // namespace io

}  // namespace husky