#pragma once

#include <string>

#include "../../base/serialization.hpp"

namespace husky {
	namespace io {

		using base::BinStream;

		class RedisSplit {
		public:
			RedisSplit();
			RedisSplit(const RedisSplit& other);

			void set_valid(bool is_valid);
			void set_hostname(const std::string& hostname);
			void set_port(int port);
			void set_key(const std::string& s);

			//void set_input_uri(const std::string& uri);
			//void set_max(const std::string& max);
			//void set_min(const std::string& min);
			//void set_ns(const std::string& ns);

			inline bool is_valid() const { return is_valid_; }
			//inline const std::string& get_input_uri() const { return input_uri_; }
			//inline const std::string& get_max() const { return max_; }
			//inline const std::string& get_min() const { return min_; }
			//inline const std::string& get_ns() const { return ns_; }
			inline const std::string& get_port() const { return port_; }
			inline const std::string& get_hostname() const { return hostname_; }
			inline const std::string get_key() const { return key_; }
		private:
			bool is_valid_;
			//std::string input_uri_;
			//std::string max_;
			//std::string min_;
			//std::string ns_;
			
			std::string hostname_;
			std::string port_;
			std::string key_;
		};

		BinStream& operator<<(BinStream& stream, RedisSplit& split);
		BinStream& operator>> (BinStream& stream, RedisSplit& split);

	}  // namespace io
}  // namespace husky
