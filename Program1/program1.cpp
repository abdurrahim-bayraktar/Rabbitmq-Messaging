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
    bsl::shared_ptr<rmqa::VHost> vhost = rabbit.createVHostConnection("program1",
                                                                      vhostInfo.value());

    // declare a tiny topology: exchange + queue + binding
    rmqa::Topology topology;
    rmqt::ExchangeHandle exchange = topology.addExchange("message-exchange");
    rmqt::QueueHandle myQueue = topology.addQueue("program1-queue"); // This program's queue
    rmqt::QueueHandle otherQueue = topology.addQueue("program2-queue"); // Other program's queue

    // Bind queues to their respective routing keys
    topology.bind(exchange, myQueue, "to-program1"); // I receive messages sent "to-program1"
    topology.bind(exchange, otherQueue, "to-program2"); // Other program receives "to-program2"

    const uint16_t maxOutstandingConfirms = 10;

    rmqt::Result<rmqa::Producer> prodRes =
        vhost->createProducer(topology, exchange, maxOutstandingConfirms);

    if (!prodRes)
    {
        std::cerr << "Failed to create producer\n";
        return 1;
    }

    bsl::shared_ptr<rmqa::Producer> producer = prodRes.value();

    // Consumer listens to MY queue
    rmqt::Result<rmqa::Consumer> consRes = vhost->createConsumer
    (
        topology,
        myQueue, // Listen to my own queue
        [](rmqp::MessageGuard& guard)
        {
            const rmqt::Message& m = guard.message();
            const uint8_t* p = m.payload();
            std::string s(reinterpret_cast<const char*>(p), m.payloadSize());
            std::cout << "Received: '" << s << "'\n";
            std::cout.flush();
            guard.ack();
        }
    );

    if (!consRes)
    {
        std::cerr << "Failed to create consumer\n";
        return 1;
    }

    std::cout << "Program 1 ready. Messages you type will be sent to Program 2.\n";
    std::cout.flush();

    // Give time for connections to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::string body;
    while (getline(cin, body))
    {
        if (body.empty()) continue;

        rmqt::Message msg(
            bsl::make_shared<bsl::vector<uint8_t>>(body.cbegin(), body.cend()));

        // Send TO the other program (routing key "to-program2")
        auto status = producer->send(
            msg,
            "to-program2", // This routes to program2-queue
            [](const rmqt::Message& message,
               const bsl::string& routingKey,
               const rmqt::ConfirmResponse& response)
            {
                if (response.status() == rmqt::ConfirmResponse::ACK)
                {
                    std::cout << "Message sent to Program 2: " << message.guid() << "\n";
                    std::cout.flush();
                }
                else
                {
                    std::cerr << "Message NOT confirmed: " << message.guid() << "\n";
                }
            });

        if (status != rmqp::Producer::SENDING)
        {
            std::cerr << "Send failed\n";
            return 1;
        }

        producer->waitForConfirms();
    }

    return 0;
}
