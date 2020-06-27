//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Bohm/Bohm.h"
#include "protocol/Bohm/BohmHelper.h"
#include "protocol/Bohm/BohmMessage.h"
#include "protocol/Bohm/BohmPartitioner.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Workload> class BohmExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = BohmTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using ProtocolType = Bohm<DatabaseType>;

  using MessageType = BohmMessage;
  using MessageFactoryType = BohmMessageFactory;
  using MessageHandlerType = BohmMessageHandler;

  BohmExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
               std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(coordinator_id, context.coordinator_num),
        workload(coordinator_id, db, random, partitioner),
        init_transaction(false),
        random(id), // make sure each worker has a different seed.
        sleep_random(reinterpret_cast<uint64_t>(this)),
        protocol(db, partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~BohmExecutor() = default;

  void start() override {

    LOG(INFO) << "BohmExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "BohmExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Bohm_Analysis);

      n_started_workers.fetch_add(1);
      generate_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Execute
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Bohm_Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      insert_write_sets();
      n_complete_workers.fetch_add(1);
      // wait to insert
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Bohm_Insert) {
        process_request();
      }

      n_started_workers.fetch_add(1);
      run_transactions();
      n_complete_workers.fetch_add(1);
      // wait to execute
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Bohm_Execute) {
        process_request();
      }

      n_started_workers.fetch_add(1);
      garbage_collect();
      n_complete_workers.fetch_add(1);
      // wait to gc
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Bohm_GC) {
        process_request();
      }
    }
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
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

  void generate_transactions() {
    uint32_t cur_epoch = epoch.load();
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (!context.same_batch || !init_transaction) {
        // generate transaction
        auto partition_id = random.uniform_dist(0, context.partition_num - 1);
        transactions[i] =
            workload.next_transaction(context, partition_id, storages[i]);
        prepare_transaction(*transactions[i]);
      } else {
        // a soft reset
        transactions[i]->network_size = 0;
        transactions[i]->load_read_count();
        transactions[i]->clear_execution_bit();
      }
      transactions[i]->set_id(epoch, i);
    }
    init_transaction = true;
  }

  void prepare_transaction(TransactionType &txn) {

    setup_prepare_handlers(txn);
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY) {
      txn.abort_no_retry = true;
      n_abort_no_retry.fetch_add(1);
    }

    if (context.same_batch) {
      txn.save_read_count();
    }

    // setup handlers for execution
    setup_execute_handlers(txn);
    txn.execution_phase = true;
  }

  void setup_prepare_handlers(TransactionType &txn) {
    txn.local_index_read_handler = [this](std::size_t table_id,
                                          std::size_t partition_id,
                                          const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id);
      BohmHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  };

  void setup_execute_handlers(TransactionType &txn) {
    txn.read_handler = [this, &txn](BohmRWKey &readKey, std::size_t tid,
                                    uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();
      bool local_index_read = readKey.get_local_index_read_bit();
      bool local_read = false;

      if (this->partitioner.has_master_partition(partition_id)) {
        local_read = true;
      }

      ITable *table = db.find_table(table_id, partition_id);
      if (local_read || local_index_read) {
        // set tid meta_data
        auto row = table->search_prev(key, tid);
        std::atomic<uint64_t> &placeholder = *std::get<0>(row);

        if (context.bohm_single_spin) {
          for (;;) {
            bool success = BohmHelper::is_placeholder_ready(placeholder);
            if (success) {
              BohmHelper::read(row, value, table->value_size());
              readKey.clear_read_request_bit();
              break;
            }
          }
        } else {
          bool success = BohmHelper::is_placeholder_ready(placeholder);
          if (success) {
            BohmHelper::read(row, value, table->value_size());
            readKey.clear_read_request_bit();
          } else {
            txn.abort_read_not_ready = true;
          }
        }
      } else {
        auto coordinatorID = this->partitioner.master_coordinator(partition_id);
        txn.network_size += MessageFactoryType::new_read_message(
            *(this->messages[coordinatorID]), *table, tid, key_offset, key);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.setup_process_requests_in_execution_phase();

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  };

  void insert_write_sets() {
    for (auto i = 0u; i < transactions.size(); i++) {
      if (transactions[i]->abort_no_retry) {
        continue;
      }
      TransactionType &t = *transactions[i].get();
      std::vector<BohmRWKey> &writeSet = t.writeSet;
      for (auto k = 0u; k < writeSet.size(); k++) {
        auto &writeKey = writeSet[k];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();
        auto table = db.find_table(tableId, partitionId);
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        if (partitioner.has_master_partition(partitionId) &&
            BohmHelper::partition_id_to_worker_id(
                partitionId, context.worker_num, context.coordinator_num) ==
                id) {
          table->insert(key, value, t.id);
        } else {
        }
      }
    }
  }

  void run_transactions() {

    auto run_transaction = [this](TransactionType *transaction) {
      if (transaction->abort_no_retry) {
        return;
      }

      // spin until the reads are ready
      for (;;) {
        process_request();
        auto result = transaction->execute(id);
        n_network_size += transaction->network_size;
        if (result == TransactionResult::READY_TO_COMMIT) {
          protocol.commit(*transaction, messages);
          n_commit.fetch_add(1);
          auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - transaction->startTime)
                  .count();
          percentile.add(latency);
          break;
        } else if (result == TransactionResult::ABORT) {
          protocol.abort(*transaction, messages);
          n_abort_lock.fetch_add(1);
          if (context.sleep_on_retry) {
            std::this_thread::sleep_for(std::chrono::microseconds(
                sleep_random.uniform_dist(0, context.sleep_time)));
          }
        } else {
          CHECK(false)
              << "abort no retry transactions should not be scheduled.";
        }
      }
      flush_messages();
    };

    if (context.bohm_local) {
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        if (!partitioner.has_master_partition(transactions[i]->partition_id)) {
          continue;
        }
        run_transaction(transactions[i].get());
      }
    } else {
      for (auto i = id + coordinator_id * context.worker_num;
           i < transactions.size();
           i += context.worker_num * context.coordinator_num) {
        run_transaction(transactions[i].get());
      }
    }
  }

  void garbage_collect() {
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      TransactionType &t = *transactions[i].get();
      std::vector<BohmRWKey> &writeSet = t.writeSet;
      for (auto k = 0u; k < writeSet.size(); k++) {
        auto &writeKey = writeSet[k];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();
        auto table = db.find_table(tableId, partitionId);
        if (partitioner.has_master_partition(partitionId)) {
          auto key = writeKey.get_key();
          table->garbage_collect(key);
        }
      }
    }
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
                              transactions);
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  BohmPartitioner partitioner;
  WorkloadType workload;
  bool init_transaction;
  RandomType random, sleep_random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, ITable &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace aria