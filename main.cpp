#include <iostream>
#include <thread>
#include <fstream>
#include <istream>
#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <queue>
#include <algorithm>
#include <iterator>
#include <mutex>
#include <atomic>
#include <condition_variable>

using namespace std;

struct ThreadHelper
{
    std::mutex dataMutex;
    std::condition_variable dataCondVar;
};

template<class D>
inline long long to_us(const D& d){
   return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

inline std::chrono::high_resolution_clock::time_point getCurrentTimeFenced(){
   std::atomic_thread_fence(std::memory_order_seq_cst);
   auto res_time = std::chrono::high_resolution_clock::now();
   std::atomic_thread_fence(std::memory_order_seq_cst);
   return res_time;
}

void mergeThread(std::queue<std::map<string, size_t>> &queueToMerge, ThreadHelper &helper)
{
    std::map<string, size_t> mapToMerge;
    while (true)
    {
        std::unique_lock<std::mutex> ul(helper.dataMutex);
        if (queueToMerge.empty())
            helper.dataCondVar.wait(ul, [&queueToMerge](){return !queueToMerge.empty();});

        if (queueToMerge.front().empty())
            break;

        auto temp = std::move(queueToMerge.front());
        queueToMerge.pop();
        ul.unlock();

        for(const auto &[k, v]: temp)
            mapToMerge[std::move(k)]+=v;
    }

    ofstream outfile ("../newText.txt");
    outfile << "common size: " << mapToMerge.size() << std::endl;

    for (const auto &[k, v]: mapToMerge)
        outfile << k << " -> " << v << endl;
    outfile.close();
}

void worker(std::queue<std::vector<std::string>> *toCount, std::queue<map<string, size_t>> &mapToMerge, ThreadHelper &mergeHelper, ThreadHelper *helper)
{
    while(true)
    {
        std::unique_lock<std::mutex> ul(helper->dataMutex);
        if (toCount->empty())
            helper->dataCondVar.wait(ul, [&toCount](){return !toCount->empty();});

        if (toCount->front().empty())
            return;
        auto strings(std::move(toCount->front()));
        toCount->pop();
        ul.unlock();

        auto wMap = map<string, size_t>();
        for (auto &word : strings)
        {
            std::transform(word.begin(), word.end(), word.begin(), ::tolower);

            word.erase(remove_if(word.begin(), word.end(), [](char c)
            { return !isalpha(c); }), word.end());

            ++wMap[std::move(word)];
        }

        if (wMap.empty())
            continue;
        std::lock_guard<std::mutex> gl(mergeHelper.dataMutex);
        mapToMerge.push(std::move(wMap));
        mergeHelper.dataCondVar.notify_one();
    }
}

void manager(std::queue<std::vector<string>> &wordsToCount, std::condition_variable &dataReadyVar, std::mutex &dataMutex)
{
    std::vector<std::pair<std::thread, ThreadHelper*>> threadsPool;
    long threadsCount = thread::hardware_concurrency();
    std::vector<std::queue<std::vector<std::string>>*> threadsQueue;
    std::queue<std::map<string, size_t>> queueToMerge;

    ThreadHelper mergeHelper;

    auto mThread = std::thread(mergeThread, ref(queueToMerge), ref(mergeHelper));

    while (true)
    {
        unique_lock<std::mutex> ul(dataMutex);
        if (wordsToCount.empty())
            dataReadyVar.wait(ul, [&wordsToCount](){return !wordsToCount.empty();});
        ul.unlock();

        //creating new threads
        if (threadsCount)
        {
            if (!threadsQueue.empty())
            {
                if (auto min = std::min_element(threadsQueue.begin(), threadsQueue.end(),
                                                [](const std::queue<std::vector<std::string>> *val1, const std::queue<std::vector<std::string>> *val2) {
                                                return val1->size() < val2->size();});
                        (min != threadsQueue.end()) && ((*min)->size() == 0))
                {
                    if (wordsToCount.front().empty())
                    {
                        for (size_t i = 0; i < threadsPool.size(); ++i)
                        {
                            threadsPool[i].second->dataMutex.lock();
                            threadsQueue[i]->push({});
                            threadsPool[i].second->dataMutex.unlock();
                            threadsPool[i].second->dataCondVar.notify_one();
                        }
                        break;
                    }

                    ul.lock();
                    auto words = std::move(wordsToCount.front());
                    wordsToCount.pop();
                    ul.unlock();

                    auto helper = threadsPool.at(std::distance(threadsQueue.begin(), min)).second;

                    helper->dataMutex.lock();
                    (*min)->push(words);
                    helper->dataMutex.unlock();
                    helper->dataCondVar.notify_one();

                    continue;
                }
            }

            threadsQueue.push_back(new std::queue<std::vector<std::string>>());
            if (wordsToCount.front().empty())
            {
                for (size_t i = 0; i < std::thread::hardware_concurrency(); ++i)
                {
                    threadsPool[i].second->dataMutex.lock();
                    threadsQueue[i]->push({});
                    threadsPool[i].second->dataMutex.unlock();
                    threadsPool[i].second->dataCondVar.notify_one();
                }
                break;
            }

            ul.lock();
            threadsQueue.back()->push(std::move(wordsToCount.front()));
            ul.unlock();

            auto threadHelper = new ThreadHelper;
            threadsPool.push_back(std::make_pair(std::thread(worker, threadsQueue.back(), ref(queueToMerge), ref(mergeHelper), threadHelper), threadHelper));
            threadsCount--;
            ul.lock();
            wordsToCount.pop();
            ul.unlock();
        }
        else
        {
            if (wordsToCount.front().back().empty())
            {
                for (size_t i = 0; i < std::thread::hardware_concurrency(); ++i)
                {
                    threadsPool[i].second->dataMutex.lock();
                    threadsQueue[i]->push({});
                    threadsPool[i].second->dataMutex.unlock();
                    threadsPool[i].second->dataCondVar.notify_one();
                }
                break;
            }

            auto minElem = std::min_element(threadsQueue.begin(), threadsQueue.end(),
                             [](const std::queue<std::vector<std::string>> *val1, const std::queue<std::vector<std::string>> *val2) {
                return val1->size() > val2->size();
            });
            ul.lock();
            auto words = std::move(wordsToCount.front());
            wordsToCount.pop();
            ul.unlock();

            auto helper = threadsPool.at(std::distance(threadsQueue.begin(), minElem)).second;

            helper->dataMutex.lock();
            (*minElem)->push(words);
            helper->dataMutex.unlock();
            helper->dataCondVar.notify_one();

        }
    }

    for (auto th = threadsPool.begin(); th != threadsPool.end(); ++th)
        th->first.join();

    for (auto &iter : threadsQueue)
        delete iter;
    for (auto &iter : threadsPool)
        delete iter.second;

    mergeHelper.dataMutex.lock();
    queueToMerge.push({});
    mergeHelper.dataCondVar.notify_one();
    mergeHelper.dataMutex.unlock();

    mThread.join();
}

int main()
{    
    auto startTime = getCurrentTimeFenced();
    ifstream file("../text.txt");

    if (!file)
        return 1;

    const int wordsPiece = 26820;
    std::queue<std::vector<string>> wordsForCount;
    std::mutex managerMutex;
    std::condition_variable dataReadyVar;
    vector<string> tokens;
    std::thread dataManager(manager, ref(wordsForCount), ref(dataReadyVar), ref(managerMutex));
    {
        int wordsCounter = 0;
        string word;
        while (file >> word)
        {
            wordsCounter++;
            tokens.push_back(std::move(word));
            if (!tokens.empty() && tokens.size() >= wordsPiece)
            {
                managerMutex.lock();
                wordsForCount.push(std::move(tokens));
                managerMutex.unlock();
                dataReadyVar.notify_one();
                tokens.clear();
            }
        }
        if (!tokens.empty())
        {
            managerMutex.lock();
            wordsForCount.push(std::move(tokens));
            managerMutex.unlock();
            dataReadyVar.notify_one();
            tokens.clear();
        }
        managerMutex.lock();
        wordsForCount.push({});
        dataReadyVar.notify_one();
        managerMutex.unlock();
        file.close();
    }

    dataManager.join();

    cout << "exec time: " << to_us(getCurrentTimeFenced() - startTime) << endl;
}
