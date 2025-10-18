/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#ifndef CIRCULAR_QUEUE_H
#define CIRCULAR_QUEUE_H

#include <memory>
#include <utility>

template <typename T>
class CircularQueue
{
public:
    CircularQueue(size_t capacity = 8) : head_(0), cnt_(0), capacity_(capacity)
    {
        vec_ = std::make_unique<T[]>(capacity);
    }

    CircularQueue(CircularQueue &&rhs)
    {
        head_ = rhs.head_;
        cnt_ = rhs.cnt_;
        capacity_ = rhs.capacity_;
        vec_ = std::move(rhs.vec_);
    }

    CircularQueue &operator=(CircularQueue &&rhs)
    {
        if (this != &rhs)
        {
            head_ = rhs.head_;
            cnt_ = rhs.cnt_;
            capacity_ = rhs.capacity_;
            vec_ = std::move(rhs.vec_);
        }
        return *this;
    }

    CircularQueue(const CircularQueue &rhs) = delete;
    CircularQueue &operator=(const CircularQueue &rhs) = delete;

    ~CircularQueue() = default;

    void Reset()
    {
        head_ = 0;
        cnt_ = 0;
        if (capacity_ > 8)
        {
            capacity_ = 8;
            vec_ = std::make_unique<T[]>(capacity_);
        }
    }

    void Enqueue(const T &item)
    {
        if (cnt_ == 0)
        {
            vec_[0] = item;
            head_ = 0;
            cnt_ = 1;
        }
        else if (cnt_ == capacity_)
        {
            size_t new_capacity = capacity_ << 1;
            std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            std::copy(
                vec_.get() + head_, vec_.get() + capacity_, new_vec.get());

            size_t half_cnt = capacity_ - head_;
            // Copy 0 --> Tail
            std::copy(vec_.get(), vec_.get() + head_, new_vec.get() + half_cnt);

            new_vec[capacity_] = item;
            cnt_ = capacity_ + 1;
            capacity_ = new_capacity;
            head_ = 0;
            vec_ = std::move(new_vec);
        }
        else
        {
            size_t tail = (head_ + cnt_) % capacity_;
            vec_[tail] = item;
            ++cnt_;
        }
    }

    void Enqueue(T &&item)
    {
        if (cnt_ == 0)
        {
            vec_[0] = std::move(item);
            head_ = 0;
            cnt_ = 1;
        }
        else if (cnt_ == capacity_)
        {
            size_t new_capacity = static_cast<size_t>(capacity_ * 1.5);
            std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            size_t end = 0;
            for (size_t idx = head_; idx < capacity_; ++idx, ++end)
            {
                new_vec[end] = std::move(vec_[idx]);
            }

            // Copy 0 --> Tail
            for (size_t idx = 0; idx < head_; ++idx, ++end)
            {
                new_vec[end] = std::move(vec_[idx]);
            }

            new_vec[capacity_] = std::move(item);
            cnt_ = capacity_ + 1;
            capacity_ = new_capacity;
            head_ = 0;
            vec_ = std::move(new_vec);
        }
        else
        {
            size_t tail = (head_ + cnt_) % capacity_;
            vec_[tail] = std::move(item);
            ++cnt_;
        }
    }

    void EnqueueAsFirst(const T &item)
    {
        if (cnt_ == 0)
        {
            vec_[0] = item;
            head_ = 0;
            cnt_ = 1;
        }
        else if (cnt_ == capacity_)
        {
            size_t new_capacity = static_cast<size_t>(capacity_ * 1.5);
            std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            std::copy(
                vec_.get() + head_, vec_.get() + capacity_, new_vec.get());

            size_t half_cnt = capacity_ - head_;
            // Copy 0 --> Tail
            std::copy(vec_.get(), vec_.get() + head_, new_vec.get() + half_cnt);

            new_vec[new_capacity - 1] = item;
            head_ = new_capacity - 1;
            cnt_ = capacity_ + 1;
            capacity_ = new_capacity;
            vec_ = std::move(new_vec);
        }
        else
        {
            if (head_ != 0)
            {
                vec_[head_ - 1] = item;
                --head_;
            }
            else
            {
                vec_[capacity_ - 1] = item;
                head_ = capacity_ - 1;
            }
            ++cnt_;
        }
    }

    void Dequeue()
    {
        if (cnt_ > 0)
        {
            ++head_;
            if (head_ == capacity_)
            {
                head_ = 0;
            }
            --cnt_;
        }
    }

    T &Peek()
    {
        return vec_[head_];
    }

    size_t Size() const
    {
        return cnt_;
    }

    size_t Capacity() const
    {
        return capacity_;
    }

    size_t MemUsage() const
    {
        return (sizeof(CircularQueue) + capacity_ * sizeof(uint64_t));
    }

    T &Get(size_t index) const
    {
        return vec_[(head_ + index) % capacity_];
    }

    void Erase(size_t index)
    {
        while (index < cnt_ - 1)
        {
            vec_[(head_ + index) % capacity_] =
                vec_[(head_ + index + 1) % capacity_];
            index++;
        }
        cnt_--;
        if (cnt_ == 0)
        {
            head_ = 0;
        }
    }

private:
    std::unique_ptr<T[]> vec_;
    size_t head_;
    size_t cnt_;
    size_t capacity_;
};
#endif // CIRCULAR_QUEUE_H