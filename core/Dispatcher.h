//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/BufferedReader.h"
#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/ControlMessage.h"
#include "core/Worker.h"
#include <atomic>
#include <glog/logging.h>
#include <thread>
#include <vector>

namespace aria {
class IncomingDispatcher {

public:
  IncomingDispatcher(std::size_t id, std::size_t group_id,
                     std::size_t io_thread_num, std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     LockfreeQueue<Message *> &coordinator_queue,
                     std::atomic<bool> &stopFlag)
      : id(id), group_id(group_id), io_thread_num(io_thread_num),
        network_size(0), workers(workers), coordinator_queue(coordinator_queue),
        stopFlag(stopFlag) {

    for (auto i = 0u; i < sockets.size(); i++) {
      buffered_readers.emplace_back(sockets[i]);
    }
  }

  void start() {
    auto numCoordinators = buffered_readers.size();
    auto numWorkers = workers.size();

    // single node test mode
    if (numCoordinators == 1) {
      return;
    }

    LOG(INFO) << "Incoming Dispatcher started, numCoordinators = "
              << numCoordinators << ", numWorkers = " << numWorkers
              << ", group id = " << group_id;

    while (!stopFlag.load()) {

      for (auto i = 0u; i < numCoordinators; i++) {
        if (i == id) {
          continue;
        }

        auto message = buffered_readers[i].next_message();

        if (message == nullptr) {
          std::this_thread::yield();
          continue;
        }

        network_size += message->get_message_length();

        // check coordinator message
        if (is_coordinator_message(message.get())) {
          coordinator_queue.push(message.release());
          CHECK(group_id == 0);
          continue;
        }

        auto workerId = message->get_worker_id();
        CHECK(workerId % io_thread_num == group_id);
        // release the unique ptr
        workers[workerId]->push_message(message.release());
        DCHECK(message == nullptr);
      }
    }

    LOG(INFO) << "Incoming Dispatcher exits, network size: " << network_size;
  }

  bool is_coordinator_message(Message *message) {
    return (*(message->begin())).get_message_type() ==
           static_cast<uint32_t>(ControlMessage::STATISTICS);
  }

  std::unique_ptr<Message> fetchMessage(Socket &socket) { return nullptr; }

private:
  std::size_t id;
  std::size_t group_id;
  std::size_t io_thread_num;
  std::size_t network_size;
  std::vector<BufferedReader> buffered_readers;
  std::vector<std::shared_ptr<Worker>> workers;
  LockfreeQueue<Message *> &coordinator_queue;
  std::atomic<bool> &stopFlag;
};

class OutgoingDispatcher {
public:
  OutgoingDispatcher(std::size_t id, std::size_t group_id,
                     std::size_t io_thread_num, std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     LockfreeQueue<Message *> &coordinator_queue,
                     std::atomic<bool> &stopFlag)
      : id(id), group_id(group_id), io_thread_num(io_thread_num),
        network_size(0), sockets(sockets), workers(workers),
        coordinator_queue(coordinator_queue), stopFlag(stopFlag) {}

  void start() {

    auto numCoordinators = sockets.size();
    auto numWorkers = workers.size();
    // single node test mode
    if (numCoordinators == 1) {
      return;
    }

    LOG(INFO) << "Outgoing Dispatcher started, numCoordinators = "
              << numCoordinators << ", numWorkers = " << numWorkers
              << ", group id = " << group_id;

    while (!stopFlag.load()) {

      // check coordinator

      if (group_id == 0 && !coordinator_queue.empty()) {
        std::unique_ptr<Message> message(coordinator_queue.front());
        bool ok = coordinator_queue.pop();
        CHECK(ok);
        sendMessage(message.get());
      }

      for (auto i = group_id; i < numWorkers; i += io_thread_num) {
        dispatchMessage(workers[i]);
      }
      std::this_thread::yield();
    }

    LOG(INFO) << "Outgoing Dispatcher exits, network size: " << network_size;
  }

  void sendMessage(Message *message) {
    auto dest_node_id = message->get_dest_node_id();
    DCHECK(dest_node_id >= 0 && dest_node_id < sockets.size() &&
           dest_node_id != id);
    DCHECK(message->get_message_length() == message->data.length())
        << message->get_message_length() << " " << message->data.length();

    sockets[dest_node_id].write_n_bytes(message->get_raw_ptr(),
                                        message->get_message_length());

    network_size += message->get_message_length();
  }

  void dispatchMessage(const std::shared_ptr<Worker> &worker) {

    Message *raw_message = worker->pop_message();
    if (raw_message == nullptr) {
      return;
    }
    // wrap the message with a unique pointer.
    std::unique_ptr<Message> message(raw_message);
    // send the message
    sendMessage(message.get());
  }

private:
  std::size_t id;
  std::size_t group_id;
  std::size_t io_thread_num;
  std::size_t network_size;
  std::vector<Socket> &sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  LockfreeQueue<Message *> &coordinator_queue;
  std::atomic<bool> &stopFlag;
};

} // namespace aria
