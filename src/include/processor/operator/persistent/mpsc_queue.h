#pragma once

// Based on the following design:
// https://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue

#include <atomic>

namespace kuzu {
namespace processor {

// Producers are completely wait-free.
template<typename T>
class MPSCQueue {
    struct Node {
        T data;
        std::atomic<Node*> next;

        explicit Node(T data) : data(std::move(data)), next(nullptr) {}
    };

    // Head is always present, but always has dummy data. This ensures that it is always easy to
    // append to the list, without branching in the methods.
    Node* head;
    std::atomic<Node*> tail;

public:
    MPSCQueue() : head(nullptr), tail(nullptr) {
        // Allocate a dummy element.
        Node* stub = new Node(T());
        head = stub;

        // Ordering doesn't matter.
        tail.store(stub, std::memory_order_relaxed);
    }
    MPSCQueue(const MPSCQueue& other) = delete;
    MPSCQueue(MPSCQueue&& other) : head(other.head) {
        other.head = nullptr;

        auto* othertail = other.tail.exchange(nullptr, std::memory_order_relaxed);
        tail.store(othertail, std::memory_order_relaxed);
    }

    // NOTE: It is NOT guaranteed that the result of a push() is accessible to a thread that calls
    // pop() after the push(), because of implementation details. See the body of the function for
    // details.
    void push(T elem) {
        Node* node = new Node(std::move(elem));
        // ORDERING: must acquire any updates to prev before modifying it, and release our updates
        // to node for other producers.
        Node* prev = tail.exchange(node, std::memory_order_acq_rel);
        // NOTE: If the thread is suspended here, then ALL FUTURE push() calls will be INACCESSIBLE
        // by pop() calls until the next line runs. In order to guarantee that a push() is visible
        // to a thread that calls pop(), ALL push() calls must have completed.
        // ORDERING: must make updates visible to consumers.
        prev->next.store(node, std::memory_order_release);
    }

    // NOTE: It is NOT safe to call pop() from multiple threads without synchronization.
    bool pop(T& elem) {
        // ORDERING: Acquire any updates made by producers.
        // Note that head is accessed only by the single consumer, so accesses to it need not be
        // synchronized.
        Node* next = head->next.load(std::memory_order_acquire);
        if (next == nullptr) {
            return false;
        }
        // Free the old element.
        delete head;
        head = next;
        elem = std::move(head->data);
        // Now the current head has dummy data in it again (i.e., whatever was leftover after the
        // move()).
        return true;
    }

    // Drain the queue. All operations on the queue MUST have finished. I.e., there must be NO
    // push() or pop() operations in progress of any kind.
    ~MPSCQueue() {
        // If we were moved out of, return.
        if (!head) {
            return;
        }

        T dummy;
        while (pop(dummy)) {}
        KU_ASSERT(head == tail.load(std::memory_order_relaxed));
        delete head;
    }
};

} // namespace processor
} // namespace kuzu
