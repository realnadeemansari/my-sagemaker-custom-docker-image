
import asyncio

from azure.servicebus.aio import ServiceBusClient
from azure.identity.aio import DefaultAzureCredential

FULLY_QUALIFIED_NAMESPACE = "https://" + "cedidspoc-dataingestion" + ".servicebus.windows.net"
QUEUE_NAME = "base_endorce_queue"
QUEUE_PROCESS_BATCH_SIZE=5

credential = DefaultAzureCredential()

async def run():
    # create a Service Bus client using the connection string
    async with ServiceBusClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        credential=credential,
        logging_enable=True) as servicebus_client:

        async with servicebus_client:
            # get the Queue Receiver object for the queue
            receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME)
            async with receiver:
                received_msgs = await receiver.receive_messages(max_wait_time=5, max_message_count=20)
                for msg in received_msgs:
                    print("Received: " + str(msg))
                    # complete the message so that the message is removed from the queue
                    await receiver.complete_message(msg)

        # Close credential when no longer needed.
        await credential.close()

asyncio.run(run())
