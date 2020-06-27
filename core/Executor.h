//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/FastSleep.h"
#include "common/Percentile.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Workload, class Protocol> class Executor : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType = typename ProtocolType::MessageHandlerType;

  using StorageType = typename WorkloadType::StorageType;

  Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        workload(coordinator_id, db, random, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    message_stats.resize(messageHandlers.size(), 0);
    message_sizes.resize(messageHandlers.size(), 0);
  }

  ~Executor() = default;

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    StorageType storage;
    uint64_t last_seed = 0;

    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    n_started_workers.fetch_add(1);
    bool retry_transaction = false;

    do {
      process_request();

      if (!partitioner->is_backup()) {
        // backup node stands by for replication
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {

          auto partition_id = get_partition_id();

          transaction =
              workload.next_transaction(context, partition_id, storage);
          setupHandlers(*transaction);
        }

        auto result = transaction->execute(id);
        if (result == TransactionResult::READY_TO_COMMIT) {
          bool commit = protocol.commit(*transaction, messages);
          if (transaction->distributed_transaction) {
            simulate_2pc_durable_cost();
          }
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            if (transaction->si_in_serializable) {
              n_si_in_serializable.fetch_add(1);
            }
            if (transaction->local_validated) {
              n_local.fetch_add(1);
            }
            retry_transaction = false;
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - transaction->startTime)
                    .count();
            percentile.add(latency);
            if (transaction->distributed_transaction) {
              dist_latency.add(latency);
            } else {
              local_latency.add(latency);
            }
          } else {
            if (transaction->abort_lock) {
              n_abort_lock.fetch_add(1);
            } else {
              DCHECK(transaction->abort_read_validation);
              n_abort_read_validation.fetch_add(1);
            }
            if (context.sleep_on_retry) {
              std::this_thread::sleep_for(std::chrono::microseconds(
                  random.uniform_dist(0, context.sleep_time)));
            }
            random.set_seed(last_seed);
            retry_transaction = true;
          }
        } else {
          protocol.abort(*transaction, messages);
          n_abort_no_retry.fetch_add(1);
        }
      }

      status = static_cast<ExecutorStatus>(worker_status.load());
    } while (status != ExecutorStatus::STOP);

    n_complete_workers.fetch_add(1);

    // once all workers are stop, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      process_request();
    }

    process_request();
    n_complete_workers.fetch_add(1);

    LOG(INFO) << "Executor " << id << " exits.";
  }

  void onExit() override {

    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%). dist txn latency: " << dist_latency.nth(50)
              << " us (50%) " << dist_latency.nth(75) << " us (75%) "
              << dist_latency.nth(95) << " us (95%) " << dist_latency.nth(99)
              << " us (99%). local txn latency: " << local_latency.nth(50)
              << " us (50%) " << local_latency.nth(75) << " us (75%) "
              << local_latency.nth(95) << " us (95%) " << local_latency.nth(99)
              << " us (99%).";

    if (id == 0) {
      for (auto i = 0u; i < message_stats.size(); i++) {
        LOG(INFO) << "message stats, type: " << i
                  << " count: " << message_stats[i]
                  << " total size: " << message_sizes[i];
      }
      percentile.save_cdf(context.cdf_path);
    }
  }

  std::size_t get_partition_id() {

    std::size_t partition_id;

    if (context.partitioner == "pb") {
      partition_id = random.uniform_dist(0, context.partition_num - 1);
    } else {
      auto partition_num_per_node =
          context.partition_num / context.coordinator_num;
      partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                         context.coordinator_num +
                     coordinator_id;
    }
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void push_message(Message *message) override { in_queue.push(message); }

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

  std::size_t process_request() {

    std::size_t size = 0;

    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        ITable *table = db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());

        messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              transaction.get());

        message_stats[type]++;
        message_sizes[type] += messagePiece.get_message_length();
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

  virtual void setupHandlers(TransactionType &txn) = 0;

protected:
  void simulate_2pc_durable_cost() {
    if (context.durable_write_cost > 0) {
      FastSleep::sleep_for(context.durable_write_cost * 2);
    }
  }

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
  DatabaseType &db;
  const ContextType &context;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile, dist_latency, local_latency;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &, TransactionType *)>>
      messageHandlers;
  std::vector<std::size_t> message_stats, message_sizes;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace aria