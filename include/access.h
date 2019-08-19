#ifndef _ACCESS_H
#define _ACCESS_H

#include <vector>
#include <stdint.h>
#include <iostream>

// create the arguments the i/o RPC will need:  we generate a bulk description
// from a vector of address-size pairs, send a list of file offsets and send a
// list of file lengths
//
// instead of a 'target' in the access, we will have a collection of accesses indexed by target
//
struct access {
    // thallium bulk described by a vector of (address, len) 'pair'
    // build that as we process the memory and file lists so we don't have to
    // loop through the description lists twice
    std::vector<std::pair<void *, std::size_t>> mem_vec;
    std::vector<off_t> offset;
    // 'len' is always same as the size_t in the (address, len) pair
    std::vector<uint64_t> len;

    void print() {
        std::cout << "    mem: ";
        for (auto x: mem_vec)
            std::cout << x.first << " " << x.second << " ";
        std::cout << std::endl;
        std::cout << "    file offset: ";
        for (auto x: offset)
            std::cout << x << " ";
        std::cout << std::endl;
        std::cout << "    file length: ";
        for (auto x: len)
            std::cout << x << " ";
        std::cout << std::endl;
    }
};
#endif
