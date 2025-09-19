//
// Created by user on 9/18/25.
//
#include <rmqa_connectionstring.h>
#include <rmqa_producer.h>
#include <rmqa_rabbitcontext.h>
#include <rmqa_topology.h>
#include <rmqt_exchange.h>
#include <rmqt_message.h>
#include <rmqt_result.h>

#include <iostream>
#include <string>
#include <thread>

using namespace BloombergLP;
using namespace std;

int main()
{
    const char* AMQP_URI = "amqp://hello:hello@localhost:5672/hello-vhost";

    rmqa::RabbitContext rabbit;

    bsl::optional<rmqt::VHostInfo> vhostInfo = rmqa::ConnectionString::parse(AMQP_URI);
    if (!vhostInfo)
    {
        std::cerr << "Bad AMQP URI\n";
        return 1;
    }

    // returns quickly; actual connect happens asynchronously
    bsl::shared_ptr<rmqa::VHost> vhost = rabbit.createVHostConnection("program2",
                                                                      vhostInfo.value());



    // declare a tiny topology: exchange + queue + binding
    // NOTE: Must be IDENTICAL to Program 1's topology
    rmqa::Topology topology;
    rmqt::ExchangeHandle exchange = topology.addExchange("topicMessage-exchange", rmqt::ExchangeType::TOPIC);
    rmqt::QueueHandle program1Queue = topology.addQueue("program1-queue");
    rmqt::QueueHandle program2Queue = topology.addQueue("program2-queue");
    rmqt::QueueHandle program3Queue = topology.addQueue("program3-queue");
    rmqt::QueueHandle returnQueue = topology.addQueue("return-queue");


    // Bind queues to their respective routing keys (same as Program 1)
    topology.bind(exchange, program1Queue, "all.to-program1");
    topology.bind(exchange, program2Queue, "all.to-program2");
    topology.bind(exchange, program3Queue, "all.to-program3");

    topology.bind(exchange, program1Queue, "all");
    topology.bind(exchange, program2Queue, "all");
    topology.bind(exchange, program3Queue, "all");
    topology.bind(exchange, returnQueue, "return");

    const uint16_t maxOutstandingConfirms = 10;

    rmqt::Result<rmqa::Producer> prodRes =
        vhost->createProducer(topology, exchange, maxOutstandingConfirms);

    if (!prodRes)
    {
        std::cerr << "Failed to create producer\n";
        return 1;
    }


    bsl::shared_ptr<rmqa::Producer> producer = prodRes.value();
    string connect = "connect";
    rmqt::Message ping(
            bsl::make_shared<bsl::vector<uint8_t>>(connect.cbegin(), connect.cend()));

    auto connectStatus = producer->send
    (
        ping,
        "all", // This routes to program1-queue
        [](const rmqt::Message& message,
           const bsl::string& routingKey,
           const rmqt::ConfirmResponse& response)
        {
            if (response.status() == rmqt::ConfirmResponse::ACK)
            {
                std::cout << "Message sent to Program 1: " << message.guid() << "\n";
                std::cout.flush();
            }
            else
            {
                std::cerr << "Message NOT confirmed: " << message.guid() << "\n";
            }
        }
    );

    if (connectStatus != rmqp::Producer::SENDING)
    {
        std::cerr << "Send failed\n";
        return 1;
    }
    producer->waitForConfirms();

    rmqt::Result<rmqa::Consumer> consRes = vhost->createConsumer(
        topology,
        "return", // Listen to my own queue
        [](rmqp::MessageGuard& guard)
        {
            const rmqt::Message& m = guard.message();
            const uint8_t* p = m.payload();
            std::string s(reinterpret_cast<const char*>(p), m.payloadSize());
            std::cout << "Received: '" << s << "'\n";
            std::cout.flush();
            guard.ack();
        });

    // Consumer listens to MY queue
    rmqt::Result<rmqa::Consumer> consRes = vhost->createConsumer(
        topology,
        program1Queue, // Listen to my own queue
        [](rmqp::MessageGuard& guard)
        {
            const rmqt::Message& m = guard.message();
            const uint8_t* p = m.payload();
            std::string s(reinterpret_cast<const char*>(p), m.payloadSize());
            std::cout << "Received: '" << s << "'\n";
            std::cout.flush();
            guard.ack();
        });


    if (!consRes)
    {
        std::cerr << "Failed to create consumer\n";
        return 1;
    }



    unordered_map<int, string> selectMap{
        {1, "all.to-program1"},
        {2, "all.to-program2"},
        {3, "all.to-program3"}
    };
    cout << "please enter the number of the user you like to message to (1-3)\n";
    cout.flush();
    int messageToSelecetor;

    cin >> messageToSelecetor;
    cin.ignore(10000,'\n');

    std::cout << "Program 2 ready.\n";
    std::cout.flush();

    // Give time for connections to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string body;
    while (getline(cin, body))
    {
        if (body.empty()) continue;

        rmqt::Message msg(
            bsl::make_shared<bsl::vector<uint8_t>>(body.cbegin(), body.cend()));

        // Send TO the other program (routing key "to-program1")
        auto status = producer->send
        (
            msg,
            selectMap.at(messageToSelecetor), // This routes to program1-queue
            [](const rmqt::Message& message,
               const bsl::string& routingKey,
               const rmqt::ConfirmResponse& response)
            {
                if (response.status() == rmqt::ConfirmResponse::ACK)
                {
                    std::cout << "Message sent to Program 1: " << message.guid() << "\n";
                    std::cout.flush();
                }
                else
                {
                    std::cerr << "Message NOT confirmed: " << message.guid() << "\n";
                }
            }
        );

        if (status != rmqp::Producer::SENDING)
        {
            std::cerr << "Send failed\n";
            return 1;
        }

        producer->waitForConfirms();
    }

    return 0;
}
