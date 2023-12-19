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
#include <iostream>

#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/fast_rand.h"
#include "butil/logging.h"
#include "brpc/rdma/rdma_helper.h"
#include "brpc/server.h"
#include "brpc/channel.h"
#include "bthread/bthread.h"
#include "bvar/latency_recorder.h"
#include "bvar/variable.h"
#include "transfile.pb.h"

#ifdef BRPC_WITH_RDMA

DEFINE_int32(thread_num, 0, "How many threads are used");
DEFINE_int32(queue_depth, 1, "How many requests can be pending in the queue");
DEFINE_int32(expected_qps, 0, "The expected QPS");
DEFINE_int32(max_thread_num, 16, "The max number of threads are used");
DEFINE_int32(attachment_size, -1, "Attachment size is used (in Bytes)");
DEFINE_bool(echo_attachment, false, "Select whether attachment should be echo");
DEFINE_string(connection_type, "single", "Connection type of the channel");
DEFINE_string(protocol, "baidu_std", "Protocol type.");
DEFINE_string(server, "0.0.0.0:8002", "IP Address of server");
DEFINE_bool(use_rdma, true, "Use RDMA or not");
DEFINE_int32(rpc_timeout_ms, 2000, "RPC call timeout");
DEFINE_int32(test_seconds, 20, "Test running time");
DEFINE_int32(test_iterations, 0, "Test iterations");
DEFINE_int32(dummy_port, 8001, "Dummy server port number");


void HandleTransfileResponse(brpc::Controller *cntl, transfile::fileResponse *resp)
{
    // std::unique_ptr makes sure cntl/response will be deleted before returning.
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<transfile::fileResponse> response_guard(resp);

    if (cntl->Failed()) 
    {
        LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
        return;
    }
    if(resp->message() == "Succ")
    {
        int file_size = cntl->response_attachment().size();
        LOG(INFO) << "resp attachment size : " << file_size;

        std::ofstream file("20230323-recv.npcbuf", std::ios::binary);
        if (file.is_open()) 
        {
            file << cntl->response_attachment();
            file.close();
            LOG(INFO) << "write into file succ";
        }
        else
            LOG(INFO) << "can not open file: 20230323-recv.npcbuf"; 
    }
    else
    {
        LOG(INFO) << "server can not find the file";
    }
}


int main(int argc, char* argv[]) 
{
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize RDMA environment in advance.
    if (FLAGS_use_rdma) {
        brpc::rdma::GlobalRdmaInitializeOrDie();
    }

    brpc::Channel channel;

    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.use_rdma = FLAGS_use_rdma;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_rpc_timeout_ms;
    options.max_retry = 0;

    if (channel.Init(FLAGS_server.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }
    transfile::TransFileService_Stub stub(&channel);


    transfile::fileResponse* response = new transfile::fileResponse();
    brpc::Controller* cntl = new brpc::Controller();
    transfile::fileRequest request;
    request.set_datetime("20230323");
    

    google::protobuf::Closure* done = brpc::NewCallback(
        &HandleTransfileResponse, cntl, response);
    stub.TransFile(cntl, &request, response, done);

    // This is an asynchronous RPC, so we can only fetch the result
    // inside the callback
    sleep(1);

    return 0;
}

#else

int main(int argc, char* argv[]) {
    LOG(ERROR) << " brpc is not compiled with rdma. To enable it, please refer to https://github.com/apache/brpc/blob/master/docs/en/rdma.md";
    return 0;
}

#endif
