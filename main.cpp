#include <iostream>
#include <thread>
#include <fstream>
#include <istream>
#include <map>
#include <vector>
#include <iostream>
#include <string>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <mutex>

using namespace std;

void countWords(std::vector<string>::iterator begin, std::vector<string>::iterator end, map<string, size_t> &mapToMerge, mutex &mux)
{
    auto wMap = map<string, size_t>();

    while (begin != end)
    {
        auto str = *begin;
        std::transform(str.begin(), str.end(), str.begin(), ::tolower);

        str.erase(remove_if(str.begin(), str.end(), [](char c)
        { return !isalpha(c); }), str.end());

        ++wMap[str];
        ++begin;
    }

    mux.lock();
    for (const auto &elem : wMap)
        mapToMerge[elem.first]+=elem.second;
    mux.unlock();
}

int main(int argc, char**argv)
{
    ifstream file("../text.txt", std::fstream::in | std::fstream::out);

    if (!file)
        return 1;
    long threadsCount = 10;
    if (argc > 1)
        if (strcmp(argv[1], "-j") == 0)
            threadsCount = atoi(argv[2]);


    vector<string> tokens;
    copy(istream_iterator<string>(file),
         istream_iterator<string>(),
         back_inserter(tokens));
    file.close();

    int block = static_cast<int>(tokens.size()/threadsCount);

    map<string, size_t> counted;
    vector<thread> threads;
    mutex mux;

    std::vector<string>::iterator iter = tokens.begin();
    for (int i = 0; i < threadsCount; ++i)
    {
        if (i == threadsCount-1)
            block+=tokens.end() - (iter+block);
        threads.push_back(thread(countWords, iter, iter+block, ref(counted), ref(mux)));
        iter+=block;
    }

    for (auto &th : threads)
        th.join();

    ofstream outfile ("../newText1.txt");

    for (auto it = counted.begin(); it != counted.end(); ++it)
        outfile << it->first << " -> " << it->second << endl;
    outfile.close();

    return 0;
}
