// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include <fstream>
#include <filesystem>

#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/time.h"
#include "butil/iobuf.h"
#include "brpc/server.h"
#include "bvar/variable.h"
#include "brpc/rdma/block_pool.h"
#include "brpc/rdma/rdma_endpoint.h"
#include "brpc/rdma/rdma_helper.h"
#include "transfile.pb.h"

#ifdef BRPC_WITH_RDMA

DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_bool(use_rdma, true, "Use RDMA or not");



std::vector<std::string> datetime_list_;
std::vector<char*> npcbuf_data_list_;
std::vector<int> npcbuf_data_size_list_;
std::vector<uint32_t> data_lkey_list_;


int read_data()
{
    namespace fs = std::filesystem;

    // save all the datetime and sort them in string order
    std::string dir_path = "../npcbuf/s5m/index";
    int file_count = 0;

    if (fs::is_directory(dir_path))
    {
        for (const auto &entry : fs::directory_iterator(dir_path))
        {
            if (entry.is_regular_file())
            {
                std::string filename = entry.path().filename().string();
                if (fs::path(filename).extension() == ".npcbuf")
                {
                    size_t dot_index = filename.rfind(".");
                    if (dot_index != std::string::npos)
                    {
                        std::string datetime = filename.substr(0, dot_index);
                        datetime_list_.emplace_back(datetime);
                        file_count++;
                    }
                }
            }
        }
    }
    else
    {
        std::cerr << "dir_path is not a directory" << std::endl;
        return -1;
    }

    std::sort(datetime_list_.begin(), datetime_list_.end());
    // vector pre reserve reduce the expand move time,
    npcbuf_data_list_.reserve(file_count);
    npcbuf_data_size_list_.reserve(file_count);
    data_lkey_list_.reserve(file_count);
    
    for (const std::string &datetime : datetime_list_)
    {
        // read npcbuf file
        std::string npcbuf_file_path = dir_path + "/" + datetime + ".npcbuf";
        std::ifstream npcbuf_file(npcbuf_file_path, std::ios::binary);
        if (!npcbuf_file)
        {
            std::cerr << "can not open npcbuf file path:" << npcbuf_file_path << std::endl;
            return -1;
        }
        
        // Get the length of the file
        npcbuf_file.seekg(0, std::ios::end);
        int file_size = (int)npcbuf_file.tellg();
        npcbuf_file.seekg(0, std::ios::beg);

        // Allocate memory
        char* npcbuf_data = (char*)malloc(file_size);

        // Read file content into allocated memory
        npcbuf_file.read(npcbuf_data, file_size);
        if (!npcbuf_file) 
        {
            std::cerr << "Error reading npcbuf file." << std::endl;
            free(npcbuf_data); // Release allocated memory in case of an error
            return -1;
        }

        uint32_t lkey = brpc::rdma::RegisterMemoryForRdma(npcbuf_data, file_size);
        if(lkey == 0)
        {
            std::cerr << "rdma::registerMemoryForRdma" << std::endl;
            free(npcbuf_data); 
            return -1;
        }

        npcbuf_data_list_.emplace_back(npcbuf_data);
        npcbuf_data_size_list_.emplace_back(file_size);
        data_lkey_list_.emplace_back(lkey);
        
        npcbuf_file.close();
    }
    return 0;
}

//Recycling data
int rec_data()
{
    int file_count = npcbuf_data_list_.size();
    for(int i = 0; i < file_count; i++)
    {
        if(!npcbuf_data_list_[i])
        {
            brpc::rdma::DeregisterMemoryForRdma(npcbuf_data_list_[i]);
            free(npcbuf_data_list_[i]);
        }
    }
    return 0;
}

static void MockFree(void* buf) { }

namespace transfile {
class TransFileServiceImpl : public TransFileService {
public:
    TransFileServiceImpl() {}
    ~TransFileServiceImpl() {}

    void TransFile(google::protobuf::RpcController* cntl_base,
              const fileRequest* request,
              fileResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);

        int file_index = std::lower_bound(datetime_list_.begin(), datetime_list_.end(), 
                                        request->datetime()) - datetime_list_.begin(); 
        
        if(file_index < datetime_list_.size())
        {
            response->set_message("Succ");
            cntl->response_attachment().append_user_data_with_meta(
                npcbuf_data_list_[file_index],
                npcbuf_data_size_list_[file_index],
                MockFree,
                data_lkey_list_[file_index]
            );
            LOG(INFO) << "Received request datetime:" <<request->datetime() 
                      << " and find success";
        }
        else
        {
            response->set_message("Fail");
            LOG(INFO) << "Received request datetime:" <<request->datetime() 
                      << " and find failed";
        }
        
    }
};
}

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize RDMA environment in advance.
    if (FLAGS_use_rdma) 
    {
        brpc::rdma::GlobalRdmaInitializeOrDie();
    }

    if(read_data() < 0) 
    {
        LOG(ERROR) << "Fail to read data";
        return -1;
    }
    LOG(INFO) << "read data succ and file count: " << datetime_list_.size();

    brpc::Server server;
    transfile::TransFileServiceImpl trans_file_service_impl;

    if (server.AddService(&trans_file_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    brpc::ServerOptions options;
    options.use_rdma = FLAGS_use_rdma;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start transfileServer";
        return -1;
    }

    server.RunUntilAskedToQuit();

    rec_data();
    return 0;
}

#else


int main(int argc, char* argv[]) {
    LOG(ERROR) << " brpc is not compiled with rdma. To enable it, please refer to https://github.com/apache/brpc/blob/master/docs/en/rdma.md";
    return 0;
}

#endif
