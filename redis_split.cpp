#include <string>

#include "io/input/redis_split.hpp"
#include "base/serialization.hpp"

namespace husky {
	namespace io {

		RedisSplit::RedisSplit() : is_valid_(true) {}

		RedisSplit::RedisSplit(const RedisSplit& other) {
			is_valid_ = other.is_valid_;
			hostname_ = other.hostname_;
			port_ = other.port_;
			key_=other.key_;
		}

		void RedisSplit::set_valid(bool valid) { is_valid_ = valid; }

		void  RedisSplit::set_hostname(const std::string& hostname) { hostname_ = hostname; }


		void  RedisSplit::set_port(int port) { port_ = port; }
		void  RedisSplit::set_key(const std::string& s) { key_ = s; }

		BinStream& operator<<(BinStream& stream, RedisSplit& split) {
			
			stream << split.get_hostname() << split.get_port() << split.get_key();
			return stream;
		}

		BinStream& operator >> (BinStream& stream, RedisSplit& split) {
			
			std::string hostname;
			int port;
			std::string keys;
			stream >> hostname >> port >> keys;
			split.set_hostname(hostname);
			split.set_port(port);
			split.set_key(keys);
			return stream;
		}

	}  // namespace io
}  // namespace husky
