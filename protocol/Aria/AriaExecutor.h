//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaMessage.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Workload> class AriaExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Aria<DatabaseType>;

  using MessageType = AriaMessage;
  using MessageFactoryType = AriaMessageFactory;
  using MessageHandlerType = AriaMessageHandler;

  AriaExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
               const ContextType &context,
               std::vector<std::unique_ptr<TransactionType>> &transactions,
               std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
               std::atomic<uint32_t> &worker_status,
               std::atomic<uint32_t> &total_abort,
               std::atomic<uint32_t> &n_complete_workers,
               std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~AriaExecutor() = default;

  void start() override {

    LOG(INFO) << "AriaExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "AriaExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Aria_READ);

      n_started_workers.fetch_add(1);
      read_snapshot();
      n_complete_workers.fetch_add(1);
      // wait to Aria_READ
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_READ) {
        process_request();
      }
      process_request();
      n_complete_workers.fetch_add(1);

      // wait till Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::Aria_COMMIT) {
        std::this_thread::yield();
      }
      n_started_workers.fetch_add(1);
      commit_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Aria_COMMIT
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Aria_COMMIT) {
        process_request();
      }
      process_request();
      n_complete_workers.fetch_add(1);
    }
  }

  std::size_t get_partition_id() {

    std::size_t partition_id;

    CHECK(context.partition_num % context.coordinator_num == 0);

    auto partition_num_per_node =
        context.partition_num / context.coordinator_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1) *
                       context.coordinator_num +
                   coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void read_snapshot() {
    // load epoch
    auto cur_epoch = epoch.load();
    auto n_abort = total_abort.load();
    std::size_t count = 0;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {

      process_request();

      // if null, generate a new transaction, on this node.
      // else only reset the query

      if (transactions[i] == nullptr || i >= n_abort) {
        auto partition_id = get_partition_id();
        transactions[i] =
            workload.next_transaction(context, partition_id, storages[i]);
      } else {
        transactions[i]->reset();
      }

      transactions[i]->set_epoch(cur_epoch);
      transactions[i]->set_id(i * context.coordinator_num + coordinator_id +
                              1); // tid starts from 1
      transactions[i]->set_tid_offset(i);
      transactions[i]->execution_phase = false;
      setupHandlers(*transactions[i]);

      count++;

      // run transactions
      auto result = transactions[i]->execute(id);
      n_network_size.fetch_add(transactions[i]->network_size);
      if (result == TransactionResult::ABORT_NORETRY) {
        transactions[i]->abort_no_retry = true;
      }

      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();

    // reserve
    count = 0;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {

      if (transactions[i]->abort_no_retry) {
        continue;
      }

      count++;

      // wait till all reads are processed
      while (transactions[i]->pendingResponses > 0) {
        process_request();
      }

      transactions[i]->execution_phase = true;
      // fill in writes in write set
      transactions[i]->execute(id);

      // start reservation
      reserve_transaction(*transactions[i]);
      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
  }

  void reserve_transaction(TransactionType &txn) {

    if (context.aria_read_only_optmization && txn.is_read_only()) {
      return;
    }

    std::vector<AriaRWKey> &readSet = txn.readSet;
    std::vector<AriaRWKey> &writeSet = txn.writeSet;

    // reserve reads;
    for (std::size_t i = 0u; i < readSet.size(); i++) {
      AriaRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner->has_master_partition(partitionId)) {
        std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, readKey);
        readKey.set_tid(&tid);
        AriaHelper::reserve_read(tid, txn.epoch, txn.id);
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_reserve_message(
            *(this->messages[coordinatorID]), *table, txn.id, readKey.get_key(),
            txn.epoch, false);
      }
    }

    // reserve writes
    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      AriaRWKey &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner->has_master_partition(partitionId)) {
        std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, writeKey);
        writeKey.set_tid(&tid);
        AriaHelper::reserve_write(tid, txn.epoch, txn.id);
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_reserve_message(
            *(this->messages[coordinatorID]), *table, txn.id,
            writeKey.get_key(), txn.epoch, true);
      }
    }
  }

  void analyze_dependency(TransactionType &txn) {

    if (context.aria_read_only_optmization && txn.is_read_only()) {
      return;
    }

    const std::vector<AriaRWKey> &readSet = txn.readSet;
    const std::vector<AriaRWKey> &writeSet = txn.writeSet;

    // analyze raw

    for (std::size_t i = 0u; i < readSet.size(); i++) {
      const AriaRWKey &readKey = readSet[i];
      if (readKey.get_local_index_read_bit()) {
        continue;
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner->has_master_partition(partitionId)) {
        uint64_t tid = AriaHelper::get_metadata(table, readKey).load();
        uint64_t epoch = AriaHelper::get_epoch(tid);
        uint64_t wts = AriaHelper::get_wts(tid);
        DCHECK(epoch == txn.epoch);
        if (epoch == txn.epoch && wts < txn.id && wts != 0) {
          txn.raw = true;
          break;
        }
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_check_message(
            *(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
            readKey.get_key(), txn.epoch, false);
        txn.pendingResponses++;
      }
    }

    // analyze war and waw

    for (std::size_t i = 0u; i < writeSet.size(); i++) {
      const AriaRWKey &writeKey = writeSet[i];

      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner->has_master_partition(partitionId)) {
        uint64_t tid = AriaHelper::get_metadata(table, writeKey).load();
        uint64_t epoch = AriaHelper::get_epoch(tid);
        uint64_t rts = AriaHelper::get_rts(tid);
        uint64_t wts = AriaHelper::get_wts(tid);
        DCHECK(epoch == txn.epoch);
        if (epoch == txn.epoch && rts < txn.id && rts != 0) {
          txn.war = true;
        }
        if (epoch == txn.epoch && wts < txn.id && wts != 0) {
          txn.waw = true;
        }
      } else {
        auto coordinatorID = this->partitioner->master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_check_message(
            *(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
            writeKey.get_key(), txn.epoch, true);
        txn.pendingResponses++;
      }
    }
  }

  void commit_transactions() {
    std::size_t count = 0;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (transactions[i]->abort_no_retry) {
        continue;
      }

      count++;

      analyze_dependency(*transactions[i]);
      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();

    count = 0;
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (transactions[i]->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
        continue;
      }
      count++;

      // wait till all checks are processed
      while (transactions[i]->pendingResponses > 0) {
        process_request();
      }

      if (context.aria_read_only_optmization &&
          transactions[i]->is_read_only()) {
        n_commit.fetch_add(1);
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transactions[i]->startTime)
                .count();
        percentile.add(latency);
        continue;
      }

      if (transactions[i]->waw) {
        protocol.abort(*transactions[i], messages);
        n_abort_lock.fetch_add(1);
        continue;
      }

      if (context.aria_snapshot_isolation) {
        protocol.commit(*transactions[i], messages);
        n_commit.fetch_add(1);
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - transactions[i]->startTime)
                .count();
        percentile.add(latency);
      } else {
        if (context.aria_reordering_optmization) {
          if (transactions[i]->war == false || transactions[i]->raw == false) {
            protocol.commit(*transactions[i], messages);
            n_commit.fetch_add(1);
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() -
                    transactions[i]->startTime)
                    .count();
            percentile.add(latency);
          } else {
            n_abort_lock.fetch_add(1);
            protocol.abort(*transactions[i], messages);
          }
        } else {
          if (transactions[i]->raw) {
            n_abort_lock.fetch_add(1);
            protocol.abort(*transactions[i], messages);
          } else {
            protocol.commit(*transactions[i], messages);
            n_commit.fetch_add(1);
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() -
                    transactions[i]->startTime)
                    .count();
            percentile.add(latency);
          }
        }
      }

      if (count % context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
  }

  void setupHandlers(TransactionType &txn) {

    txn.readRequestHandler = [this, &txn](AriaRWKey &readKey, std::size_t tid,
                                          uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();
      bool local_index_read = readKey.get_local_index_read_bit();

      bool local_read = false;

      if (this->partitioner->has_master_partition(partition_id)) {
        local_read = true;
      }

      ITable *table = db.find_table(table_id, partition_id);
      if (local_read || local_index_read) {
        // set tid meta_data
        auto row = table->search(key);
        AriaHelper::set_key_tid(readKey, row);
        AriaHelper::read(row, value, table->value_size());
      } else {
        auto coordinatorID =
            this->partitioner->master_coordinator(partition_id);
        txn.network_size += MessageFactoryType::new_search_message(
            *(this->messages[coordinatorID]), *table, tid, txn.tid_offset, key,
            key_offset);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
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
  std::atomic<uint32_t> &epoch, &worker_status, &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  RandomType random;
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