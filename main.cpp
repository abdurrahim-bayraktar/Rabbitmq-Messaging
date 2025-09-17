#include <rmqa_connectionstring.h>
#include <rmqa_consumer.h>
#include <rmqa_rabbitcontext.h>
#include <rmqa_topology.h>
#include <rmqp_messageguard.h>
#include <rmqt_message.h>


#include <iostream>
#include <thread>
#include <chrono>


using namespace BloombergLP;


int main()
{
    // Put the AMQP URI directly in the file (change as needed)
    const char* AMQP_URI = "amqp://hello:hello@localhost:5672/hello-vhost";


    rmqa::RabbitContext rabbit;


    bsl::optional<rmqt::VHostInfo> vhostInfo = rmqa::ConnectionString::parse(AMQP_URI);
    if (!vhostInfo) {
        std::cerr << "Bad AMQP URI\n";
        return 1;
    }


    bsl::shared_ptr<rmqa::VHost> vhost = rabbit.createVHostConnection("simple-consumer",
    vhostInfo.value());


    // declare same tiny topology as the producer
    rmqa::Topology topology;
    rmqt::ExchangeHandle exch = topology.addExchange("hello-exchange");
    rmqt::QueueHandle queue = topology.addQueue("hello-queue");
    topology.bind(exch, queue, "hello");


    // create consumer: callback receives a MessageGuard
    rmqt::Result<rmqa::Consumer> consRes = vhost->createConsumer(
    topology,
    queue,
    // callback runs on RabbitContext's threadpool
    [](rmqp::MessageGuard &guard) {
    const rmqt::Message &m = guard.message();
    const uint8_t *p = m.payload();
    std::string s(reinterpret_cast<const char*>(p), m.payloadSize());
    std::cout << "Received: '" << s << "'\n";


    // acknowledge the message (remove from queue)
    guard.ack();
    });


    if (!consRes) {
        std::cerr << "Failed to create consumer: " << consRes.error() << "\n";
        return 1;
    }


    // keep main thread alive so consumer callbacks keep running
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }


    return 0;
}