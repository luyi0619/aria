//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "core/Context.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Worker.h"

#include <thread>

namespace aria {

class Manager : public Worker {
public:
  Manager(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag)
      : Worker(coordinator_id, id), context(context), stopFlag(stopFlag),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    worker_status.store(static_cast<uint32_t>(ExecutorStatus::STOP));
  }

  virtual void coordinator_start() {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    n_started_workers.store(0);
    n_completed_workers.store(0);
    signal_worker(ExecutorStatus::START);
    wait_all_workers_start();

    // wait till stop flag is set
    while (!stopFlag.load()) {
      std::this_thread::yield();
    }

    set_worker_status(ExecutorStatus::STOP);
    wait_all_workers_finish();
    broadcast_stop();
    wait4_stop(n_coordinators - 1);
    // process replication
    n_completed_workers.store(0);
    set_worker_status(ExecutorStatus::CLEANUP);
    wait_all_workers_finish();
    wait4_ack();
  }

  virtual void non_coordinator_start() {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    ExecutorStatus status = wait4_signal();
    DCHECK(status == ExecutorStatus::START);
    n_completed_workers.store(0);
    n_started_workers.store(0);
    set_worker_status(ExecutorStatus::START);
    wait_all_workers_start();
    wait4_stop(1);
    set_worker_status(ExecutorStatus::STOP);
    wait_all_workers_finish();
    broadcast_stop();
    wait4_stop(n_coordinators - 2);
    // process replication
    n_completed_workers.store(0);
    set_worker_status(ExecutorStatus::CLEANUP);
    wait_all_workers_finish();
    send_ack();
  }

  void wait_all_workers_finish() {
    std::size_t n_workers = context.worker_num;
    // wait for all workers to finish
    while (n_completed_workers.load() < n_workers) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void wait_all_workers_start() {
    std::size_t n_workers = context.worker_num;
    // wait for all workers to finish
    while (n_started_workers.load() < n_workers) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void set_worker_status(ExecutorStatus status) {
    worker_status.store(static_cast<uint32_t>(status));
  }

  void signal_worker(ExecutorStatus status) {

    // only the coordinator node calls this function
    DCHECK(coordinator_id == 0);
    set_worker_status(status);

    // signal to everyone
    for (auto i = 0u; i < context.coordinator_num; i++) {
      if (i == coordinator_id) {
        continue;
      }
      ControlMessageFactory::new_signal_message(*messages[i],
                                                static_cast<uint32_t>(status));
    }
    flush_messages();
  }

  ExecutorStatus wait4_signal() {
    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    signal_in_queue.wait_till_non_empty();

    std::unique_ptr<Message> message(signal_in_queue.front());
    bool ok = signal_in_queue.pop();
    CHECK(ok);

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::SIGNAL);

    uint32_t status;
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> status;

    return static_cast<ExecutorStatus>(status);
  }

  void wait4_stop(std::size_t n) {

    // wait for n stop messages

    for (auto i = 0u; i < n; i++) {

      stop_in_queue.wait_till_non_empty();

      std::unique_ptr<Message> message(stop_in_queue.front());
      bool ok = stop_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
      CHECK(type == ControlMessage::STOP);
    }
  }

  void wait4_ack() {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {

      ack_in_queue.wait_till_non_empty();

      std::unique_ptr<Message> message(ack_in_queue.front());
      bool ok = ack_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
      CHECK(type == ControlMessage::ACK);
    }
  }

  void send_stop(std::size_t node_id) {

    DCHECK(node_id != coordinator_id);

    ControlMessageFactory::new_stop_message(*messages[node_id]);

    flush_messages();
  }

  void broadcast_stop() {

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators; i++) {
      if (i == coordinator_id)
        continue;
      ControlMessageFactory::new_stop_message(*messages[i]);
    }

    flush_messages();
  }

  void send_ack() {

    // only non-coordinator calls this function
    DCHECK(coordinator_id != 0);

    ControlMessageFactory::new_ack_message(*messages[0]);
    flush_messages();
  }

  void start() override {

    if (coordinator_id == 0) {
      LOG(INFO) << "Manager(worker id = " << id
                << ") on the coordinator node started.";
      coordinator_start();
      LOG(INFO) << "Manager(worker id = " << id
                << ") on the coordinator node exits.";
    } else {
      LOG(INFO) << "Manager(worker id = " << id
                << ") on the non-coordinator node started.";
      non_coordinator_start();
      LOG(INFO) << "Manager(worker id = " << id
                << ") on the non-coordinator node exits.";
    }
  }

  void push_message(Message *message) override {

    // message will only be of type signal, C_PHASE_ACK or S_PHASE_ACK

    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());

    auto message_type =
        static_cast<ControlMessage>(messagePiece.get_message_type());

    switch (message_type) {
    case ControlMessage::SIGNAL:
      signal_in_queue.push(message);
      break;
    case ControlMessage::VECTOR:
      vector_in_queue.push(message);
      break;
    case ControlMessage::ACK:
      ack_in_queue.push(message);
      break;
    case ControlMessage::STOP:
      stop_in_queue.push(message);
      break;
    default:
      CHECK(false) << "Message type: " << static_cast<uint32_t>(message_type);
      break;
    }
  }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now -
                                                                message->time)
              .count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

protected:
  void flush_messages() {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();

      out_queue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

protected:
  const Context &context;
  std::atomic<bool> &stopFlag;
  LockfreeQueue<Message *> ack_in_queue, vector_in_queue, signal_in_queue,
      stop_in_queue, out_queue;
  std::vector<std::unique_ptr<Message>> messages;

public:
  std::atomic<uint32_t> worker_status;
  std::atomic<uint32_t> n_completed_workers;
  std::atomic<uint32_t> n_started_workers;

  std::unique_ptr<Delay> delay;
};

} // namespace aria
