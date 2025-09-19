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

    rmqa::Topology topology;
    vector<rmqt::QueueHandle> queues;
    rmqt::ExchangeHandle exchange = topology.addExchange("toppicMessage-exchange", rmqt::ExchangeType::TOPIC);
    queues.push_back(topology.addQueue("return-queue"));
    queues.push_back(topology.addQueue("program1-queue"));
    queues.push_back(topology.addQueue("program2-queue"));
    queues.push_back(topology.addQueue("program3-queue"));


    topology.bind(exchange, queues[1], "all.to-program1");
    topology.bind(exchange, queues[2], "all.to-program2");
    topology.bind(exchange, queues[3], "all.to-program3");
    topology.bind(exchange, queues[1], "all");
    topology.bind(exchange, queues[2], "all");
    topology.bind(exchange, queues[3], "all");
    topology.bind(exchange, queues[0], "return");

    const uint16_t maxOutstandingConfirms = 10;

    int messageTurn = 0;
    int thisUser = 2;
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
    while (true)
    {
        if (messageTurn % 2 == 0)
        {
            cout << "YOU: ";
            rmqt::Result<rmqa::Producer> prodRes =
                vhost->createProducer(topology, exchange, maxOutstandingConfirms);

            bsl::shared_ptr<rmqa::Producer> producer = prodRes.value();

            // Consumer listens to MY queue
            rmqt::Result<rmqa::Consumer> consRes = vhost->createConsumer(
                topology,
                queues[thisUser], // Listen to my own queue
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
            }


            // Give time for connections to establish
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            std::string body;

            getline(cin, body);

            if (body == "exit")
            {
                cout << "who would you like to message (1-3)" <<endl;
                cin >> messageToSelecetor;
                cin.ignore(10000,'\n');
                body = "";
            }

            rmqt::Message msg(
                bsl::make_shared<bsl::vector<uint8_t>>(body.cbegin(), body.cend()));


            auto status = producer->send
            (
                msg,
                selectMap.at(messageToSelecetor), // This routes to program1-queue
                [](const rmqt::Message& message,
                   const bsl::string& routingKey,
                   const rmqt::ConfirmResponse& response)
                {
                    if (response.status() == rmqt::ConfirmResponse::ACK)
                    {}
                }
            );

            if (status != rmqp::Producer::SENDING)
            {
                std::cerr << "Send failed\n";
            }
            ++messageTurn;
            producer->waitForConfirms();
        }
        else //messaging of the other user
        {

            {
                rmqt::Result<rmqa::Producer> prodRes =
                    vhost->createProducer(topology, exchange, maxOutstandingConfirms);

                bsl::shared_ptr<rmqa::Producer> producer = prodRes.value();

                // Consumer listens to MY queue
                rmqt::Result<rmqa::Consumer> consRes = vhost->createConsumer(
                    topology,
                    queues[messageToSelecetor], // Listen to my own queue
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
                }

                // Give time for connections to establish
                std::this_thread::sleep_for(std::chrono::milliseconds(500));

                std::string body;
                cout<<"Program: " << messageToSelecetor << endl;
                getline(cin, body);

                if (body == "exit")
                {
                    cout << "who would you like to message (1-3)" <<endl;
                    cin >> messageToSelecetor;
                    cin.ignore(10000,'\n');
                    body ="";
                }

                rmqt::Message msg(
                    bsl::make_shared<bsl::vector<uint8_t>>(body.cbegin(), body.cend()));


                auto status = producer->send
                (
                    msg,
                    selectMap.at(messageToSelecetor), // This routes to program1-queue
                    [](const rmqt::Message& message,
                       const bsl::string& routingKey,
                       const rmqt::ConfirmResponse& response)
                    {
                        if (response.status() == rmqt::ConfirmResponse::ACK)
                        {}
                    }
                );

                if (status != rmqp::Producer::SENDING)
                {
                    std::cerr << "Send failed\n";
                }
                producer->waitForConfirms();
                ++messageTurn;
            }
        }
    }
}
