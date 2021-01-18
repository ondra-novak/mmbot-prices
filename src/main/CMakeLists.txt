cmake_minimum_required(VERSION 2.8) 

add_executable (prices main.cpp )
target_link_libraries (prices LINK_PUBLIC userver docdblib imtjson leveldb stdc++fs pthread)
