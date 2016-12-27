
#include <map>
#include <string>
#include <vector>
#include <list>
#include "io/input/redis_split.hpp"

namespace husky {

	
	using io::RedisSplit;

	class RedisSplitAssigner {
	public:
		RedisSplitAssigner();
		void master_redis_req_handler();
		void master_redis_req_end_handler();
		virtual ~RedisSplitAssigner();
	
		
		void initialize_map(const std::string&,int);
		void create_splits(const std::string&,int,const std::string&);
		RedisSplit answer(const std::string&, int,const std::string&);
		void recieve_end(RedisSplit& split);
		
	private:
		bool need_auth_ = false;
		int end_count_;
		int split_num_;
		std::string hostname_="";
		int port_;
		

		std::vector<std::string> key_;
		std::vector<RedisSplit> splits_;
		std::vector<RedisSplit> splits_end_;
		std::list<std::pair<std::pair<std::string, int>, std::pair<int, int>> > slotmap_;
		std::list<std::pair<std::string, std::string>> master_slave_map_;
		std::set<std::string> worker_ = { "192.168.50.5","192.168.50.6","192.168.50.7","192.168.50.8","192.168.50.9" };//hard core here some problems when converting hotsname
		std::set<std::string> worker_resi_;
		std::set<std::pair<std::string,std::string>> avaiable_keys_; //keys hostname
		
	};

}  // namespace husky


