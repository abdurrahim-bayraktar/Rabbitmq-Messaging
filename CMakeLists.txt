cmake_minimum_required(VERSION 3.20)
project(rabbitmqConsumer)

set(CMAKE_CXX_STANDARD 17)  # Try 17 first
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(rmqcpp CONFIG REQUIRED)

find_package(bal CONFIG REQUIRED)


add_executable(rabbitmqConsumer program1.cpp)

target_link_libraries(rabbitmqConsumer PRIVATE bal balb-iface ball-iface balm-iface
    rmqcpp::rmq
)