#ifndef BV_CACHE
#define BV_CACHE

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <libgen.h>
#include <string.h>
#include <set>

#include <margo.h>
#include <margo-bulk-pool.h>
#include <thallium.hpp>
#include <abt-io.h>
#include <ssg.h>
#include <map>
#include <mutex>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/margo_exception.hpp>

#include <list>
namespace tl = thallium;

#define BENVOLIO_CACHE_ENABLE 1

#if BENVOLIO_CACHE_ENABLE==1
//#define BENVOLIO_CACHE_MAX_N_BLOCKS 32
//#define BENVOLIO_CACHE_MIN_N_BLOCKS 32
#define BENVOLIO_CACHE_MAX_FILE 5
//#define BENVOLIO_CACHE_MAX_BLOCK_SIZE 16777216
//#define BENVOLIO_CACHE_MAX_BLOCK_SIZE 355
#define BENVOLIO_CACHE_WRITE 1
#define BENVOLIO_CACHE_READ 0
#define BENVOLIO_CACHE_RESOURCE_CHECK_TIME 58
#define BENVOLIO_CACHE_RESOURCE_REMOVE_TIME 88
#define BENVOLIO_CACHE_STATISTICS 0
#define BENVOLIO_CACHE_STATISTICS_DETAILED 0
#define CACULATE_TIMESTAMP(current_timestamp, init_timestamp) ((int)(((current_timestamp) - (init_timestamp))/10))


static int BENVOLIO_CACHE_MAX_N_BLOCKS;
static int BENVOLIO_CACHE_MIN_N_BLOCKS;
static int BENVOLIO_CACHE_MAX_BLOCK_SIZE;
static float BENVOLIO_CACHE_WRITE_BACK_RATIO;

typedef struct {
    std::map<off_t, uint64_t>* cache_page_usage;
    int cache_page_hit_count;
    int cache_page_fetch_count;
    int write_back_count;
    int cache_block_flush_count;
    int files_register_count;
    int files_reuse_register_count;
    int cache_erased;
} Cache_counter;

typedef struct {
    Cache_counter cache_counter;
    double flush_time;
    double memcpy_time;
    double write_back_time;
    double cache_fetch_time;
    double cache_match_time;
    double cache_total_time;
} Cache_stat;

/**
 * Toolkits for describing provider's cache information.
 * Maps are all from files to cache related data structures inside Cache_file_info (more details below).
*/
typedef struct {
    abt_io_instance_id abt_id;
    std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*> *cache_table;
    std::map<std::string, std::set<off_t>*> *cache_update_table;
    std::map<std::string, tl::mutex*> *cache_mutex_table;
    std::map<std::string, tl::mutex*> *cache_mem_mutex_table;
    std::map<std::string, std::vector<off_t>*> *cache_offset_list_table;
    std::map<std::string, int> *register_table;
    std::map<std::string, int> *cache_block_reserve_table;
    std::map<std::string, double> *cache_timestamps;
    std::map<std::string, int> *fd_table;
    std::map<std::string, std::map<off_t, int>*> *cache_page_refcount_table;
    std::map<std::string, std::vector<char*>*> *cache_backup_memory_table;
    std::map<std::string, size_t> *file_size_table;
    std::map<std::string, std::map<off_t, size_t>*> *cache_page_written_table;

    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    std::map<std::string, std::map<int, Cache_counter*>*> *cache_counter_table;
    #endif
    std::map<std::string, Cache_stat*> *cache_stat_table;
    std::map<int, Cache_counter*>* cache_counter_sum;
    Cache_stat* cache_stat_sum;
    long long int max_request;
    long long int min_request;
    #endif
    double *init_timestamp;

    tl::mutex *cache_mutex;
    int *shutdown;
    int *cache_block_used;
    int ssg_rank;
} Cache_info;
/**
 * Toolkits for describing provider's cache information for a specific file.
*/
typedef struct Cache_file_info{
    /* I/O file descriptors */
    abt_io_instance_id abt_id;
    int fd;
    /* This map stores all cache blocks. The key is the file offset. The first element of the pair is the # of bytes of the cache block */
    std::map<off_t, std::pair<uint64_t, char*>*> *cache_table;
    /* Store which cache block has been modified by a write operation.*/
    std::set<off_t>* cache_update_list;
    tl::mutex *cache_mutex;

    tl::mutex *cache_mem_mutex;
    /* The rest of variables does not require a lock for their accesses*/
    int cache_evictions;
    #if BENVOLIO_CACHE_STATISTICS == 1
    Cache_stat *cache_stat;
    ABT_mutex *thread_mutex;
    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    std::map<int, Cache_counter*> *cache_counter_table;
    #endif
    #endif
    std::map<off_t, std::pair<uint64_t, char*>> *cache_page_table;
    std::map<off_t, size_t> *cache_page_written_table;
    /* The order of cache blocks fetched. When we flush a block, it starts from the first element of the list*/
    std::vector<off_t>* cache_offset_list;
    std::map<off_t, int> *cache_page_refcount_table;
    std::vector<char*>* cache_backup_memory;
    std::vector<uint64_t> *file_sizes;
    std::vector<off_t> *file_starts;

    double *init_timestamp;
    /* Read or write operation?*/
    int io_type;
    /* How many cache blocks? */
    size_t cache_block_reserved;
    int stripe_count;
    int stripe_size;
    size_t file_size;
    size_t write_max_size;
    /*Debug purpose*/
    int ssg_rank;
} Cache_file_info;

static void cache_register(Cache_info *cache_info, const std::string file, Cache_file_info *cache_file_info);
static void cache_register_lock(Cache_info *cache_info, const std::string file, Cache_file_info *cache_file_info);
static void cache_deregister_lock(Cache_info *cache_info, std::string file, Cache_file_info *cache_file_info);
static void cache_deregister(Cache_info *cache_info, std::string file, Cache_file_info *cache_file_info);
static void cache_init(Cache_info *cache_info);
static void cache_finalize(Cache_info *cache_info);
static void cache_write_back(Cache_file_info *cache_file_info);
static void cache_write_back_lock(Cache_file_info *cache_file_info);
//static void cache_flush(Cache_file_info *cache_file_info);
//static void cache_fetch(Cache_file_info cache_file_info, off_t file_start, uint64_t file_size, int stripe_size, int stripe_count);

static void cache_count_request_pages(Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size, std::set<off_t> *pages);
static void cache_count_request_extra_pages(Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size, std::set<off_t> *pages);
static size_t cache_count_requests_pages(Cache_file_info *cache_file_info, const std::vector<off_t> &file_starts, const std::vector<uint64_t> &file_sizes);
static void cache_remove_file_lock(Cache_info *cache_info, std::string file);
static int cache_exist(Cache_info *cache_info, std::string file);
static void cache_flush_all(Cache_info *cache_info, const int check_time);
static void cache_allocate_memory(Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size);
static void cache_flush_array(Cache_file_info *cache_file_info, const std::vector<off_t> *cache_offsets, const int clean_memory);

#if BENVOLIO_CACHE_STATISTICS == 1
#if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
static void cache_page_usage_log(Cache_info *cache_info, std::string file);
#endif
#endif

static char* cache_malloc(Cache_file_info *cache_file_info, const uint64_t page_size) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mem_mutex));
    if (cache_file_info->cache_backup_memory->size()) {
        char* result = cache_file_info->cache_backup_memory[0][0];
        cache_file_info->cache_backup_memory->erase(cache_file_info->cache_backup_memory->begin());
        return result;
    } else {
        return (char*) malloc(sizeof(char) * page_size);
    }
}

static void cache_free(Cache_file_info *cache_file_info, char* mem_ptr) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mem_mutex));
    cache_file_info->cache_backup_memory->push_back(mem_ptr);
}

static void cache_add_counter(Cache_counter *cache_counter1, const Cache_counter *cache_counter2) {
    cache_counter1->cache_page_hit_count += cache_counter2->cache_page_hit_count;
    cache_counter1->cache_page_fetch_count += cache_counter2->cache_page_fetch_count;
    cache_counter1->write_back_count += cache_counter2->write_back_count;
    cache_counter1->cache_block_flush_count += cache_counter2->cache_block_flush_count;
    cache_counter1->files_register_count += cache_counter2->files_register_count;
    cache_counter1->files_reuse_register_count += cache_counter2->files_reuse_register_count;
    cache_counter1->cache_erased += cache_counter2->cache_erased;
}

static void cache_add_stat(Cache_stat *cache_stat1, const Cache_stat *cache_stat2) {
    cache_add_counter(&(cache_stat1->cache_counter), &(cache_stat2->cache_counter));
    cache_stat1->flush_time += cache_stat2->flush_time;
    cache_stat1->memcpy_time += cache_stat2->memcpy_time;
    cache_stat1->cache_fetch_time += cache_stat2->cache_fetch_time;
    cache_stat1->cache_match_time += cache_stat2->cache_match_time;
    cache_stat1->write_back_time += cache_stat2->write_back_time;
    cache_stat1->cache_total_time += cache_stat2->cache_total_time;
}

static void cache_summary(Cache_info *cache_info, int ssg_rank) {
    #if BENVOLIO_CACHE_STATISTICS == 1
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    char filename[1024];
    FILE* stream;
    int max_timestamp=-1, i;

    //cache_stat for all files are summed here
    Cache_stat *cache_stat = cache_info->cache_stat_sum;
    std::map<std::string, Cache_stat*>::iterator it2;
    for ( it2 = cache_info->cache_stat_table->begin(); it2 != cache_info->cache_stat_table->end(); ++it2 ) {
        #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
        cache_page_usage_log(cache_info, it2->first);
        #endif
        cache_add_stat(cache_stat, it2->second);
    }

    printf("Rank %d summary:\n Files registered: %d\n Files register reused: %d\n Write-back function called: %d\n Cache block flushed: %d\n Cache block erased: %d\n Cache page fetch: %d\n Cache page hit: %d\n Total flush time (due to memory limit): %lf\n Total write-back-time: %lf\n Total memory copy: %lf\n Total fetch page time %lf\n maximum request size = %lld\n minimum request size = %lld\n Total match time %lf\n Total cache time %lf\n", ssg_rank, cache_stat->cache_counter.files_register_count, cache_stat->cache_counter.files_reuse_register_count, cache_stat->cache_counter.write_back_count, cache_stat->cache_counter.cache_block_flush_count, cache_stat->cache_counter.cache_erased, cache_stat->cache_counter.cache_page_fetch_count, cache_stat->cache_counter.cache_page_hit_count, cache_stat->flush_time, cache_stat->write_back_time, cache_stat->memcpy_time, cache_stat->cache_fetch_time, cache_info->max_request, cache_info->min_request, cache_stat->cache_match_time, cache_stat->cache_total_time);
    sprintf(filename, "provider_timing_log_%d.csv", ssg_rank);
    stream = fopen(filename,"w");
    fprintf(stream, "Rank %d summary:\n Files registered: %d\n Files register reused: %d\n Write-back function called: %d\n Cache block flushed: %d\n Cache block erased: %d\n Cache page fetch: %d\n Cache page hit: %d\n Total flush time (due to memory limit): %lf\n Total write-back-time: %lf\n Total memory copy: %lf\n Total fetch page time %lf\n maximum request size = %lld\n minimum request size = %lld\n Total match time %lf\n Total cache time %lf\n", ssg_rank, cache_stat->cache_counter.files_register_count, cache_stat->cache_counter.files_reuse_register_count, cache_stat->cache_counter.write_back_count, cache_stat->cache_counter.cache_block_flush_count, cache_stat->cache_counter.cache_erased, cache_stat->cache_counter.cache_page_fetch_count, cache_stat->cache_counter.cache_page_hit_count, cache_stat->flush_time, cache_stat->write_back_time, cache_stat->memcpy_time, cache_stat->cache_fetch_time, cache_info->max_request, cache_info->min_request, cache_stat->cache_match_time, cache_stat->cache_total_time);
    fclose(stream);

    //cache_counter per timestamp is summed up here
    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    std::map<std::string, std::map<int, Cache_counter*>*>::iterator it3;
    std::map<int, Cache_counter*>::iterator it4;

    for (it3 = cache_info->cache_counter_table->begin(); it3 != cache_info->cache_counter_table->end(); ++it3) {
        for ( it4 = it3->second->begin(); it4 != it3->second->end(); ++it4 ) {
            if ( cache_info->cache_counter_sum->find(it4->first) == cache_info->cache_counter_sum->end() ) {
                cache_info->cache_counter_sum[0][it4->first] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
            }
            cache_add_counter(cache_info->cache_counter_sum[0][it4->first], it4->second);
        }
    }
    
    std::map<int, Cache_counter*> *cache_counter_table = cache_info->cache_counter_sum;

    std::map<int, Cache_counter*>::iterator it;
    for (it = cache_counter_table->begin(); it != cache_counter_table->end(); ++it) {
        if (it->first > max_timestamp || max_timestamp==-1) {
            max_timestamp = it->first;
        }
    }
    //printf("maximum timestamp = %d\n", max_timestamp);

    sprintf(filename, "provider_counter_log_%d.csv", ssg_rank);
    stream = fopen(filename,"w");
    fprintf(stream,"timestamp,");
    fprintf(stream,"file_register_count,");
    fprintf(stream,"file_reuse_register_count,");
    fprintf(stream,"cache_page_fetch,");
    fprintf(stream,"cache_page_hit,");
    fprintf(stream,"write_back_count,");
    fprintf(stream,"cache_page_flush_count,");
    fprintf(stream,"cache_file_erased\n");

    for (i = 0; i <= max_timestamp; ++i) {
        fprintf(stream,"%d,", i);
        if (cache_counter_table->find(i) == cache_counter_table->end()) {
            fprintf(stream,"0, 0, 0, 0, 0, 0, 0\n",i);
            continue;
        }
        fprintf(stream,"%d,", cache_counter_table[0][i]->files_register_count);
        fprintf(stream,"%d,", cache_counter_table[0][i]->files_reuse_register_count);
        fprintf(stream,"%d,", cache_counter_table[0][i]->cache_page_fetch_count);
        fprintf(stream,"%d,", cache_counter_table[0][i]->cache_page_hit_count);
        fprintf(stream,"%d,", cache_counter_table[0][i]->write_back_count);
        fprintf(stream,"%d,", cache_counter_table[0][i]->cache_block_flush_count);
        fprintf(stream,"%d\n", cache_counter_table[0][i]->cache_erased);
    }

    fclose(stream);

    #endif
    #endif
}

static int cache_exist(Cache_info *cache_info, std::string file) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    return cache_info->cache_table->find(file) != cache_info->cache_table->end();
}


#if BENVOLIO_CACHE_STATISTICS == 1

#if BENVOLIO_CACHE_STATISTICS_DETAILED == 1

static void cache_page_usage_log(Cache_info *cache_info, std::string file) {
    FILE* stream;
    int max_timestamp=-1, i, j;
    char filename[2048];
    std::map<int, Cache_counter*> *cache_counter_table = cache_info->cache_counter_table[0][file];
    std::map<int, Cache_counter*>::iterator it;
    std::set<off_t> *cache_pages = new std::set<off_t>;
    for ( it = cache_counter_table->begin(); it != cache_counter_table->end(); ++it ) {
        if (it->first > max_timestamp || max_timestamp==-1) {
            max_timestamp = it->first;
        }
        std::map<off_t, uint64_t>::iterator it2;
        for (it2 = it->second->cache_page_usage->begin(); it2 != it->second->cache_page_usage->end(); ++it2) {
            cache_pages->insert(it2->first);
        }
    }
    sprintf(filename, "%s_provider_page_usage_%d_%lld.csv", file.c_str(), cache_info->ssg_rank, (long long int)ABT_get_wtime());
    //printf("---------------------------------------ssg_rank %d entered cache_page_usage_log %s, cache page size = %ld\n", cache_info->ssg_rank, filename, cache_pages->size());
    stream = fopen(filename,"w");
    fprintf(stream,"timestamp");
    //printf("timestamp");
    std::set<off_t>::iterator it3;
    for (it3 = cache_pages->begin(); it3 != cache_pages->end(); ++it3) {
        //fprintf(stream,",1");
        long long int temp = (long long int) *it3;
        //printf(",%lld",temp);
        fprintf(stream, ",%lld", temp);
        //printf("ssg_rank = %d and %lld \n", cache_info->ssg_rank, temp);
    }
    fprintf(stream, "\n");
    //printf("\n");

    for (i = 0; i <= max_timestamp; ++i) {
        fprintf(stream, "%d", i);
        //printf("%d", i);
        if (cache_counter_table->find(i) == cache_counter_table->end()) {
            for ( j = 0; j < (int) cache_pages->size(); ++j ) {
                fprintf(stream,",0");
                //printf(",0");
            }
            //printf("\n");
            fprintf(stream,"\n");
            continue;
        }
        std::set<off_t>::iterator it4;
        for (it4 = cache_pages->begin(); it4 != cache_pages->end(); ++it4) {
            if (cache_counter_table[0][i]->cache_page_usage->find(*it4) != cache_counter_table[0][i]->cache_page_usage->end()) {
                long long unsigned temp = (long long unsigned) cache_counter_table[0][i]->cache_page_usage[0][*it4];
                fprintf(stream, ",%llu", temp );
                //printf(",%llu", temp);
                //fprintf(stream,",1");
            } else {
                fprintf(stream,",0");
                //printf(",0");
            }
        }
        //printf("\n");
        fprintf(stream,"\n");
    }
    //printf("---------------------------------------------------\n");
    fclose(stream);
    delete cache_pages;

}
#endif

#endif
/**
 This function has to be called by functions that lock the cache_info.
*/
static void cache_remove_file(Cache_info *cache_info, std::string file) {
    #if BENVOLIO_CACHE_STATISTICS == 1
    cache_info->cache_stat_table[0][file]->cache_counter.cache_erased++;

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    int t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_info->init_timestamp[0]);
    if (cache_info->cache_counter_table[0][file]->find(t_index) != cache_info->cache_counter_table[0][file]->end() ) {
        cache_info->cache_counter_table[0][file][0][t_index]->cache_erased++;
    } else {
        cache_info->cache_counter_table[0][file][0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
        cache_info->cache_counter_table[0][file][0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
        cache_info->cache_counter_table[0][file][0][t_index]->cache_erased++;
    }
    
    std::map<int, Cache_counter*>* cache_counter_map = cache_info->cache_counter_table[0][file];
    std::map<int, Cache_counter*>::iterator it;
    Cache_counter *temp;
    for ( it = cache_counter_map->begin(); it != cache_counter_map->end(); ++it ) {
        if (cache_info->cache_counter_sum->find(it->first) == cache_info->cache_counter_sum->end()) {
            temp = (Cache_counter*) calloc(1, sizeof(Cache_counter));
            cache_info->cache_counter_sum[0][it->first] = temp;
        } else {
            temp = cache_info->cache_counter_sum[0][it->first];
        }
        cache_add_counter(temp, it->second);
    }
    cache_page_usage_log(cache_info, file);
    delete cache_info->cache_counter_table[0][file];
    cache_info->cache_counter_table->erase(file);
    #endif
    cache_add_stat(cache_info->cache_stat_sum, cache_info->cache_stat_table[0][file]);
    delete cache_info->cache_stat_table[0][file];
    cache_info->cache_stat_table->erase(file);
    #endif
    //printf("ssg_rank %d -------------------------removing cache for file %s\n", cache_info->ssg_rank, file.c_str());
    std::map<off_t, std::pair<uint64_t, char*>*>::iterator it2;
    std::map<off_t, std::pair<uint64_t, char*>*> *cache_file_table = cache_info->cache_table[0][file];
    std::vector<char*> *cache_backup_memory = cache_info->cache_backup_memory_table[0][file];
    std::vector<char*>::iterator it3;

    Cache_file_info cache_file_info;
    cache_file_info.cache_backup_memory = cache_backup_memory;
    cache_file_info.cache_mem_mutex = cache_info->cache_mem_mutex_table[0][file];

    for ( it2 = cache_file_table->begin(); it2 != cache_file_table->end(); ++it2 ) {
        //free(it2->second->second);
        cache_free(&cache_file_info, it2->second->second);
        //cache_backup_memory->push_back(it2->second->second);
        //printf("load address %llu\n", (long long unsigned) it2->second->second);
        delete it2->second;
    }
    for ( it3 = cache_backup_memory->begin(); it3 != cache_backup_memory->end(); ++it3 ) {
        //printf("free address %llu\n", (long long unsigned) *it3);
        free(*it3);
    }
    std::map<off_t, int> *cache_file_page_refcount_table = cache_info->cache_page_refcount_table[0][file];

    delete cache_backup_memory;
    delete cache_file_table;
    delete cache_info->cache_page_written_table[0][file];
    delete cache_info->cache_update_table[0][file];
    delete cache_info->cache_mutex_table[0][file];
    delete cache_file_page_refcount_table;
    delete cache_info->cache_offset_list_table[0][file];
    delete cache_info->cache_mem_mutex_table[0][file];

    cache_info->cache_table->erase(file);
    cache_info->cache_page_written_table->erase(file);
    cache_info->cache_mutex_table->erase(file);
    cache_info->cache_page_refcount_table->erase(file);
    cache_info->cache_update_table->erase(file);
    cache_info->cache_offset_list_table->erase(file);
    cache_info->cache_block_used[0] -= cache_info->cache_block_reserve_table[0][file];
    cache_info->cache_block_reserve_table->erase(file);
    cache_info->register_table->erase(file);
    cache_info->cache_timestamps->erase(file);
    cache_info->fd_table->erase(file);
    cache_info->cache_backup_memory_table->erase(file);
    cache_info->cache_mem_mutex_table->erase(file);
}

static void cache_remove_file_lock(Cache_info *cache_info, std::string file) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    if (cache_info->cache_table->find(file) != cache_info->cache_table->end() && cache_info->register_table[0][file] == 0) {
        cache_remove_file(cache_info, file);
    }
}

/*
 * This function requires cache_register for cache_file_info. In addition, it needs abt_io and file descriptor.
 * Other parameters for cache_file_info like stripe count/size are not useful.
*/

static void cache_write_back(Cache_file_info *cache_file_info) {
    //printf("write back function reached\n");

    std::list<abt_io_op_t *> ops;
    std::list<ssize_t> rets;
    off_t cache_offset;
    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    cache_file_info->cache_stat->cache_counter.write_back_count++;
    int t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
    if (cache_file_info->cache_counter_table->find(t_index) != cache_file_info->cache_counter_table->end() ) {
        cache_file_info->cache_counter_table[0][t_index]->write_back_count++;
    } else {
        cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
        cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
        cache_file_info->cache_counter_table[0][t_index]->write_back_count++;
    }
    #endif

    double time = ABT_get_wtime();
    #endif

    std::set<off_t>::iterator it;
    for (it = cache_file_info->cache_update_list->begin(); it != cache_file_info->cache_update_list->end(); ++it) {
        cache_offset = *it;
        if (cache_file_info->cache_table->find(cache_offset) == cache_file_info->cache_table->end()) {
            printf("offset not find in cache, critical error\n");
            break;
        }

        rets.push_back(-1);
        abt_io_op_t * write_op = abt_io_pwrite_nb(cache_file_info->abt_id, cache_file_info->fd, cache_file_info->cache_table[0][cache_offset]->second, cache_file_info->cache_table[0][cache_offset]->first, cache_offset, &(rets.back()) );
        ops.push_back(write_op);
    }

    for (auto x : ops) {
        abt_io_op_wait(x);
        abt_io_op_free(x);
    }
    cache_file_info->cache_update_list->clear();
    cache_file_info->cache_page_written_table->clear();

    ops.clear();
    rets.clear();
    #if BENVOLIO_CACHE_STATISTICS == 1
    cache_file_info->cache_stat->write_back_time = ABT_get_wtime() - time;
    #endif
}

static void cache_write_back_lock(Cache_file_info *cache_file_info) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    cache_write_back(cache_file_info);
}

/*
 * This function must be called in a function that lock cache_info.
 * We do not have to lock the file cache because nothing will happen if the file cache is currently registered by RPCs (checking the register table count)
 * When we add arguments to cache_file_info, just remember to add to this function as well, in addition to the register function.
*/
static void cache_flush_all(Cache_info *cache_info, const int check_time) {
    std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*>::iterator it;
    std::vector<std::string> filenames;
    for ( it = cache_info->cache_table->begin(); it != cache_info->cache_table->end(); ++it ) {
        filenames.push_back(it->first);
    }
    std::vector<std::string>::iterator it2;
    for ( it2 = filenames.begin(); it2 != filenames.end(); ++it2) {
        if (cache_info->register_table[0][*it2] == 0 && (!check_time || (ABT_get_wtime() - cache_info->cache_timestamps[0][*it2]) > BENVOLIO_CACHE_RESOURCE_REMOVE_TIME)) {
            // We just need to register and write-back, so these arguments are enough, but this is not a good design because cache_file_info2 cannot be used for operations other than write-back.
            // Missing stripe count/size, ssg_rank, io_type here. These are not needed for register and write-back operations.
            Cache_file_info cache_file_info2;
            cache_file_info2.fd = cache_info->fd_table[0][*it2];
            cache_file_info2.abt_id = cache_info->abt_id;

            cache_register(cache_info, *it2, &cache_file_info2);
            cache_write_back(&cache_file_info2);
            cache_deregister(cache_info, *it2, &cache_file_info2);
            cache_remove_file(cache_info, *it2);
        }
    }
}

/*
 * Simple remove all pages by write-back.
*/
static void cache_flush_all_lock(Cache_info *cache_info, const int check_time) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    cache_flush_all(cache_info, check_time);
}

static void cache_request_counter(Cache_info *cache_info, const std::vector<uint64_t> &file_sizes) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    #if BENVOLIO_CACHE_STATISTICS == 1
    unsigned i;
    long long int request_size = 0;
    for ( i = 0; i < file_sizes.size(); ++i ) {
        request_size += file_sizes[i];
    }
    if ( cache_info->max_request == -1 || cache_info->max_request < request_size ) {
        cache_info->max_request = request_size;
    } else if (  cache_info->min_request == -1 || cache_info->min_request > request_size) {
        cache_info->min_request = request_size;
    }

    #endif
}

/*
  This function can only be called when the cache_eviction flag is on.
*/
static int cache_page_register2(Cache_file_info *cache_file_info, const std::vector<std::vector<off_t>*> *file_starts_array, const std::vector<std::vector<uint64_t>*> *file_sizes_array, const std::vector<off_t> *pages, size_t page_index) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    unsigned i, remove_counter, remaining_pages;

    // Count the remaining pages that has to be created for this RPC. This can be differnt from what we know in cache_page_register function since time has elapsed and the cache table could be different.

    remaining_pages = 0;
    for ( i = page_index; i < pages->size(); ++i ) {
        if (cache_file_info->cache_table->find(pages[0][i]) == cache_file_info->cache_table->end()) {
            remaining_pages++;
        }
    }
    off_t flush_offset;
    std::vector<off_t> *flush_offsets = new std::vector<off_t>;
    std::vector<off_t>::iterator it2;
    // Entering this condition means that we are out of memory budget, we need to try to remove pages that are not currently used by anyone.

    if (cache_file_info->cache_table->size() + remaining_pages > cache_file_info->cache_block_reserved) {
        // Remove as many pages as we can. We may not be able to remove enough pages due to other threads are actively using them, but we will see how we can do here.
        remove_counter = cache_file_info->cache_table->size() + remaining_pages - cache_file_info->cache_block_reserved;
        //printf("reached remove condition. cache table size = %ld, remaining_pages = %u, reserved = %d\n", cache_file_info->cache_table->size(), remaining_pages, cache_file_info->cache_block_reserved);
        it2 = cache_file_info->cache_offset_list->begin();
        while (it2 != cache_file_info->cache_offset_list->end()) {
            flush_offset = *it2;
            if (cache_file_info->cache_page_refcount_table[0][flush_offset] == 0) {
                flush_offsets->push_back(flush_offset);
                if ( flush_offsets->size() == remove_counter ) {
                    break;
                }
            }
            ++it2;
        }
        if (flush_offsets->size()) {
            cache_flush_array(cache_file_info, flush_offsets, 1);
        }
    }

    delete flush_offsets;

    // We have to process at least one page, regardless of memory budget. This could be a bad idea, but there is no way to get around of this.
    for ( i = 0; i < file_starts_array[0][page_index]->size(); ++i ) {
        cache_allocate_memory(cache_file_info, file_starts_array[0][page_index][0][i], file_sizes_array[0][page_index][0][i]);
        cache_file_info->file_starts->push_back(file_starts_array[0][page_index][0][i]);
        cache_file_info->file_sizes->push_back(file_sizes_array[0][page_index][0][i]);
    }
    page_index++;
    // As long as we have budgets, we keep allocating as many pages as possible.

    while (cache_file_info->cache_table->size() <= cache_file_info->cache_block_reserved && page_index < pages->size()) {
        for ( i = 0; i < file_starts_array[0][page_index]->size(); ++i ) {
            cache_allocate_memory(cache_file_info, file_starts_array[0][page_index][0][i], file_sizes_array[0][page_index][0][i]);
            cache_file_info->file_starts->push_back(file_starts_array[0][page_index][0][i]);
            cache_file_info->file_sizes->push_back(file_sizes_array[0][page_index][0][i]);
        }
        page_index++;
    }
    /* Remeber this cache_page_table is cleared inside cache_page_deregister2. Hence whatever is currently in cache_page_table should be what we are using in this register function. */
    std::map<off_t, std::pair<uint64_t, char*>>::iterator it3;
    for ( it3 = cache_file_info->cache_page_table->begin(); it3 != cache_file_info->cache_page_table->end(); ++it3 ) {
        cache_file_info->cache_page_refcount_table[0][it3->first]++;
    }
    return page_index;
}

/*
  This function can only be called when the cache_eviction flag is on.
*/
static void cache_page_deregister2(Cache_file_info *cache_file_info, const std::vector<off_t> *pages) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    std::vector<off_t> cache_offsets;

    cache_file_info->file_starts->clear();
    cache_file_info->file_sizes->clear();
    std::vector<off_t>::iterator it;
    std::map<off_t, std::pair<uint64_t, char*>>::iterator it3;
    off_t cache_offset;
    for ( it3 = cache_file_info->cache_page_table->begin(); it3 != cache_file_info->cache_page_table->end(); ++it3 ) {
        cache_offset = it3->first;
        cache_file_info->cache_page_refcount_table[0][cache_offset]--;
        // Sometimes it is better to write-back a cache page when it is almost full.
        if ( cache_file_info->cache_page_refcount_table[0][cache_offset] == 0 && cache_file_info->cache_page_written_table[0][cache_offset] > cache_file_info->cache_table[0][cache_offset]->first * BENVOLIO_CACHE_WRITE_BACK_RATIO) {
            cache_offsets.push_back(cache_offset);
        }
    }
    // Write-back when necessary.
    if (cache_offsets.size()) {
        //printf("rank %d urgent write-back started at deregister2, io type is %d !!!!!!!!!!\n",cache_file_info->ssg_rank, cache_file_info->io_type);
        cache_flush_array(cache_file_info, &cache_offsets, 0);
    }

    cache_file_info->cache_page_table->clear();
}

/* Align file requests to the boundaries of cache pages. */
static void cache_partition_request(Cache_file_info *cache_file_info, const std::vector<off_t> &file_starts, const std::vector<uint64_t> &file_sizes, std::vector<std::vector<off_t>*> **file_starts_array, std::vector<std::vector<uint64_t>*> **file_sizes_array, std::vector<off_t> **pages_vec) {
    //std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    std::set<off_t> *pages = new std::set<off_t>;
    off_t cache_offset;
    size_t i, j, last_request_index;
    uint64_t cache_size, cache_size2;
    int stripe_size = cache_file_info->stripe_size;

    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }

    /* Figure out how many pages we are touching*/
    for ( i = 0; i < file_starts.size(); ++i ) {
        cache_count_request_pages(cache_file_info, file_starts[i], file_sizes[i], pages);
    }
    //printf("total number of pages = %ld, page reserved = %ld\n", pages.size(), cache_file_info->cache_block_reserved);

    file_starts_array[0] = new std::vector<std::vector<off_t>*>(pages->size());
    file_sizes_array[0] = new std::vector<std::vector<uint64_t>*>(pages->size());
    pages_vec[0] = new std::vector<off_t>(pages->size());
    std::set<off_t>::iterator it;
    last_request_index = 0;
    j = 0;
    for ( it = pages->begin(); it != pages->end(); ++it ) {
        cache_offset = *it;

        file_starts_array[0][0][j] = new std::vector<off_t>;
        file_sizes_array[0][0][j] = new std::vector<uint64_t>;
        pages_vec[0][0][j] = cache_offset;

        //printf("cache offset = %llu\n", (long long unsigned)cache_offset);
        cache_size2 = MIN(cache_size, stripe_size - (cache_offset % stripe_size));
        for ( i = last_request_index; i < file_starts.size(); ++i ) {
            if (file_starts[i] >= cache_offset && file_starts[i] < cache_offset + cache_size2) {
                //Start position inside cache page.
                file_starts_array[0][0][j]->push_back(file_starts[i]);
                if (file_starts[i] + file_sizes[i] <= cache_offset + cache_size2) {
                    //Request fall into the page entirely.
                    file_sizes_array[0][0][j]->push_back(file_sizes[i]);
                    last_request_index = i + 1;
                    // This request is done, we do not need it anymore later.
                } else {
                    //Request tail can be out of this page, we need to chop the request into two halves. We want the head here. The request is not finished here !!!
                    file_sizes_array[0][0][j]->push_back(cache_offset + cache_size2 - file_starts[i]);
                    last_request_index = i;
                    break;
                }
            } else if ( file_starts[i] < cache_offset && file_starts[i] + file_sizes[i] > cache_offset ) {
                //Start position is before the cache page, but part of its tail is inside the current page, we want a partial tail.
                file_starts_array[0][0][j]->push_back(cache_offset);
                if (file_starts[i] + file_sizes[i] <= cache_offset + cache_size2) {
                    // The end of current request tail fall into this page, we are done with this request here.
                    file_sizes_array[0][0][j]->push_back(file_sizes[i] - (cache_offset - file_starts[i]));
                    last_request_index = i + 1;
                } else {
                    // We used the entire page and this request is not done yet !!!!
                    file_sizes_array[0][0][j]->push_back(cache_size2);
                    last_request_index = i;
                }
            } else {
                // This request does not intersect this page, we leave this request.
                break;
            }
        }
        j++;

    }
    delete pages;
}

/**
   Register the cache pages that are going to be used here.
   cache_page_eviction flag is going to determine if we need to partition the requests due to memory limit.
*/
static void cache_page_register(Cache_file_info *cache_file_info, const std::vector<off_t> &file_starts, const std::vector<uint64_t> &file_sizes, std::vector<std::vector<off_t>*> **file_starts_array, std::vector<std::vector<uint64_t>*> **file_sizes_array, std::vector<off_t> **pages_vec) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    size_t pages = cache_count_requests_pages(cache_file_info, file_starts, file_sizes);
    cache_file_info->cache_page_table = new std::map<off_t, std::pair<uint64_t, char*>>;
    if (pages + cache_file_info->cache_table->size() > cache_file_info->cache_block_reserved ) {
        //printf("ssg_rank %d entering cache eviction strategies, pages needed = %llu, page used = %lu, budgets = %llu\n", cache_file_info->ssg_rank, (long long unsigned)pages, (long long unsigned) cache_file_info->cache_table->size() ,(long long unsigned) cache_file_info->cache_block_reserved);
        cache_file_info->cache_evictions = 1;
        cache_partition_request(cache_file_info, file_starts, file_sizes, file_starts_array, file_sizes_array, pages_vec);
    } else {
        //printf("ssg_rank %d entering cache preregistration scheme, pages needed = %llu, page used = %lu, budgets = %llu\n", cache_file_info->ssg_rank, (long long unsigned)pages, (long long unsigned) cache_file_info->cache_table->size() ,(long long unsigned) cache_file_info->cache_block_reserved);

        cache_file_info->cache_evictions = 0;
        for ( size_t i = 0; i < file_starts.size(); ++i ) {
            cache_allocate_memory(cache_file_info, file_starts[i], file_sizes[i]);
        }
        std::map<off_t, std::pair<uint64_t, char*>>::iterator it;
        for ( it = cache_file_info->cache_page_table->begin(); it != cache_file_info->cache_page_table->end(); ++it ) {
            cache_file_info->cache_page_refcount_table[0][it->first]++;
        }

    }
}

/**
    Exit the cache_page_register block.
*/
static void cache_page_deregister(Cache_file_info *cache_file_info, std::vector<std::vector<off_t>*> *file_starts_array, std::vector<std::vector<uint64_t>*> *file_sizes_array, std::vector<off_t> *pages) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    std::vector<off_t> cache_offsets;
    off_t cache_offset;

    std::map<off_t, std::pair<uint64_t, char*>>::iterator it3;

    if (cache_file_info->cache_evictions) {
        //printf("entered deregister for cache eviction\n");
        std::vector<std::vector<uint64_t>*>::iterator it;
        std::vector<std::vector<off_t>*>::iterator it2;
        for (it = file_sizes_array->begin(); it != file_sizes_array->end(); ++it){
            delete *it;
        }
        for (it2 = file_starts_array->begin(); it2 != file_starts_array->end(); ++it2){
            delete *it2;
        }
        delete pages;
        delete file_sizes_array;
        delete file_starts_array;
    } else {
        for ( it3 = cache_file_info->cache_page_table->begin(); it3 != cache_file_info->cache_page_table->end(); ++it3 ) {
            cache_offset = it3->first;
            cache_file_info->cache_page_refcount_table[0][cache_offset]--;
            // Sometimes it is better to write-back a cache page when it is almost full.
            if ( cache_file_info->cache_page_refcount_table[0][cache_offset] == 0 && cache_file_info->cache_page_written_table[0][cache_offset] > cache_file_info->cache_table[0][cache_offset]->first * BENVOLIO_CACHE_WRITE_BACK_RATIO) {
                cache_offsets.push_back(cache_offset);
            }
        }

    }
    // Write-back when necessary.
    if (cache_offsets.size()) {
        //printf("rank %d urgent write-back started at here, io type is %d !!!!!!!!!!\n",cache_file_info->ssg_rank, cache_file_info->io_type);
        cache_flush_array(cache_file_info, &cache_offsets, 0);
    }

    delete cache_file_info->cache_page_table;
}
 
/*
 * Set size function. We simply reset the record for file size.
 * TODO: Ditch all cache pages beyond the file size.
*/
static void cache_set_file_size(Cache_info *cache_info, const std::string &file, int64_t file_size) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    /* We are keep tracking the "real" file size that is on the disk, so we only shrink, but not enlarging the cache file size until the point request force the enlargement. */
    if (cache_info->file_size_table[0][file] > file_size) {
        cache_info->file_size_table[0][file] = file_size;
    }
}

/*
 * Initialize a cache_file_info here. It is possible that we should have some additional arguments for I/O to be set as well.
 * We can assume this function register cache_info with some new vectors (or retrieve from it).
 * Always remember to call deregister function when cache_file_info is no longer used.
*/
static void cache_register(Cache_info *cache_info, const std::string file, Cache_file_info *cache_file_info) {

    cache_file_info->init_timestamp = cache_info->init_timestamp;
    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    cache_file_info->cache_counter_table = new std::map<int, Cache_counter*>;
    #endif

    cache_file_info->cache_stat = (Cache_stat*) calloc(1, sizeof(Cache_stat));
    #endif
    if (cache_info->cache_table->find(file) == cache_info->cache_table->end()) {
        cache_file_info->cache_table = new std::map<off_t, std::pair<uint64_t, char*>*>;
        cache_file_info->cache_update_list = new std::set<off_t>;
        cache_file_info->cache_mutex = new tl::mutex;
        cache_file_info->cache_mem_mutex = new tl::mutex;
        cache_file_info->cache_offset_list = new std::vector<off_t>;
        cache_file_info->cache_page_refcount_table = new std::map<off_t, int>;
        cache_file_info->cache_backup_memory = new std::vector<char*>;
        cache_file_info->cache_page_written_table = new std::map<off_t, size_t>;

        /* When we are out of cache space, we are going to remove all file caches that are not currently processed.*/
        if (cache_info->cache_block_used[0] >= BENVOLIO_CACHE_MAX_N_BLOCKS) {
            printf("ssg_rank %d entered eager cache write-back mechanism! page used = %d, budget = %d\n", cache_info->ssg_rank, cache_info->cache_block_used[0], BENVOLIO_CACHE_MAX_N_BLOCKS);
            //cache_flush_all(cache_info, 0);
        }

        cache_file_info->cache_block_reserved = MAX(BENVOLIO_CACHE_MIN_N_BLOCKS, (BENVOLIO_CACHE_MAX_N_BLOCKS - cache_info->cache_block_used[0])/2);

        if (cache_file_info->io_type == BENVOLIO_CACHE_READ) {
            cache_file_info->cache_block_reserved = MIN(cache_file_info->cache_block_reserved, (cache_file_info->file_size + BENVOLIO_CACHE_MAX_BLOCK_SIZE - 1) / BENVOLIO_CACHE_MAX_BLOCK_SIZE );
        }

        //printf("ssg_rank = %d, File %s, Registered cache block size of %llu, previously used %llu\n", cache_info.ssg_rank, file.c_str(), (long long unsigned)cache_file_info->cache_block_reserved, (long long unsigned)cache_info.cache_block_used[0]);

        cache_info->cache_block_used[0] += cache_file_info->cache_block_reserved;
        cache_info->register_table[0][file] = 1;
        cache_info->cache_timestamps[0][file] = ABT_get_wtime();

        cache_info->cache_table[0][file] = cache_file_info->cache_table;
        cache_info->cache_update_table[0][file] = cache_file_info->cache_update_list;
        cache_info->cache_mutex_table[0][file] = cache_file_info->cache_mutex;
        cache_info->cache_mem_mutex_table[0][file] = cache_file_info->cache_mem_mutex;
        cache_info->cache_offset_list_table[0][file] = cache_file_info->cache_offset_list;
        cache_info->cache_block_reserve_table[0][file] = cache_file_info->cache_block_reserved;
        cache_info->cache_page_refcount_table[0][file] = cache_file_info->cache_page_refcount_table;
        cache_info->cache_backup_memory_table[0][file] = cache_file_info->cache_backup_memory;
        cache_info->cache_page_written_table[0][file] = cache_file_info->cache_page_written_table;
	cache_info->fd_table[0][file] = cache_file_info->fd;

        /* We store the file size when this request is a read request. 
         * For a write request, we use the maximum of request offset and history file size to determine the actual size. */
        if (cache_file_info->io_type == BENVOLIO_CACHE_READ) {
            cache_info->file_size_table[0][file] = cache_file_info->file_size;
        } else {
            cache_info->file_size_table[0][file] = MAX(cache_file_info->file_size, cache_file_info->write_max_size);
        }

        //printf("ssg_rank %d entered here checkpoint 1\n", cache_file_info->ssg_rank);
        #if BENVOLIO_CACHE_STATISTICS == 1

        #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
        //cache_info->cache_counter_table[0][file] = cache_file_info->cache_counter_table;
        cache_info->cache_counter_table[0][file] = new std::map<int, Cache_counter*>;
        #endif

        cache_info->cache_stat_table[0][file] = (Cache_stat*) calloc(1, sizeof(Cache_stat));
        #endif
	//Record file descriptor to global cache table (we may need to get it in unavailable contexts). We should only do this when the file is first met.
        #if BENVOLIO_CACHE_STATISTICS == 1
        cache_file_info->cache_stat->cache_counter.files_register_count++;

        #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
        int t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_info->init_timestamp[0]);
        if (cache_file_info->cache_counter_table->find(t_index) != cache_file_info->cache_counter_table->end() ) {
            cache_file_info->cache_counter_table[0][t_index]->files_register_count++;
        } else {
            cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
            cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
            cache_file_info->cache_counter_table[0][t_index]->files_register_count++;
        }
        #endif

        #endif

    } else {
        cache_file_info->cache_table = cache_info->cache_table[0][file];
        cache_file_info->cache_update_list = cache_info->cache_update_table[0][file];
        cache_file_info->cache_mutex = cache_info->cache_mutex_table[0][file];
        cache_file_info->cache_mem_mutex = cache_info->cache_mem_mutex_table[0][file];
        cache_file_info->cache_offset_list = cache_info->cache_offset_list_table[0][file];
        cache_file_info->cache_page_refcount_table = cache_info->cache_page_refcount_table[0][file];
        cache_file_info->cache_backup_memory = cache_info->cache_backup_memory_table[0][file];
        cache_file_info->file_size = cache_info->file_size_table[0][file];
        cache_file_info->cache_page_written_table = cache_info->cache_page_written_table[0][file];

        if (cache_file_info->io_type == BENVOLIO_CACHE_WRITE && cache_file_info->file_size < cache_file_info->write_max_size) {
            cache_info->file_size_table[0][file] = cache_file_info->write_max_size;
            cache_file_info->file_size = cache_file_info->write_max_size;
        }

        cache_file_info->cache_block_reserved = cache_info->cache_block_reserve_table[0][file];

        //printf("block used = %ld - %ld\n", cache_info->cache_block_used[0], cache_file_info->cache_block_reserved);
        cache_info->cache_block_used[0] -= cache_file_info->cache_block_reserved;
        cache_file_info->cache_block_reserved = MAX(BENVOLIO_CACHE_MIN_N_BLOCKS, (BENVOLIO_CACHE_MAX_N_BLOCKS - cache_info->cache_block_used[0])/2);
        if (cache_file_info->io_type == BENVOLIO_CACHE_READ) {
            cache_file_info->cache_block_reserved = MIN(cache_file_info->cache_block_reserved, (cache_file_info->file_size + BENVOLIO_CACHE_MAX_BLOCK_SIZE) / BENVOLIO_CACHE_MAX_BLOCK_SIZE );
        }
        //printf("block used = %ld + %ld\n", cache_info->cache_block_used[0], cache_file_info->cache_block_reserved);
        cache_info->cache_block_used[0] += cache_file_info->cache_block_reserved;
        cache_info->cache_block_reserve_table[0][file] = cache_file_info->cache_block_reserved;
/*
        #if BENVOLIO_CACHE_STATISTICS == 1

        #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
        cache_file_info->cache_counter_table = cache_info->cache_counter_table[0][file];
        #endif

        cache_file_info->cache_stat = cache_info->cache_stat_table[0][file];
        #endif
*/
        cache_info->register_table[0][file] += 1;
        cache_info->cache_timestamps[0][file] = ABT_get_wtime();

        #if BENVOLIO_CACHE_STATISTICS == 1
        cache_file_info->cache_stat->cache_counter.files_reuse_register_count++;

        #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
        int t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_info->init_timestamp[0]);
        if (cache_file_info->cache_counter_table->find(t_index) != cache_file_info->cache_counter_table->end() ) {
            cache_file_info->cache_counter_table[0][t_index]->files_reuse_register_count++;
        } else {
            cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
            cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
            cache_file_info->cache_counter_table[0][t_index]->files_reuse_register_count++;
        }
        #endif

        #endif

        //printf("ssg_rank = %d, File %s, Retrieving registered cache block size of %llu\n", cache_info.ssg_rank, file.c_str(), (long long unsigned)cache_file_info->cache_block_reserved);
    }
    //printf("-------------------------------------ssg_rank = %d, file_size = %ld, write_max_size = %ld, io_type = %d\n", cache_info->ssg_rank, cache_file_info->file_size, cache_file_info->write_max_size, cache_file_info->io_type);
}

static void cache_register_lock(Cache_info *cache_info, const std::string file, Cache_file_info *cache_file_info) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    cache_register(cache_info, file, cache_file_info);
}

static void cache_deregister(Cache_info *cache_info, std::string file, Cache_file_info *cache_file_info) {
    cache_info->register_table[0][file] -= 1;
    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    int t_index;
    std::map<int, Cache_counter*>* global_cache_counter_table = cache_info->cache_counter_table[0][file];
    std::map<off_t, uint64_t> *local_cache_page_usage,*global_cache_page_usage;
    std::map<int, Cache_counter*>::iterator it;
    std::map<off_t, uint64_t>::iterator it2;
    for ( it = cache_file_info->cache_counter_table->begin(); it != cache_file_info->cache_counter_table->end(); ++it ) {
        t_index = it->first;
        if (global_cache_counter_table->find(t_index) == global_cache_counter_table->end()) {
            global_cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
            global_cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
        }
        cache_add_counter(global_cache_counter_table[0][t_index], cache_file_info->cache_counter_table[0][t_index]);

        global_cache_page_usage = global_cache_counter_table[0][t_index]->cache_page_usage;
        // Whenever we initialize a time slot, this cache_page_usage table must be initilized even if it is needed at the moment, so we are safe here.
        local_cache_page_usage = it->second->cache_page_usage;
        for ( it2 = local_cache_page_usage->begin(); it2 != local_cache_page_usage->end(); ++it2 ) {
            if (global_cache_page_usage->find(it2->first) == global_cache_page_usage->end()) {
                global_cache_page_usage[0][it2->first] = it2->second;
            } else {
                global_cache_page_usage[0][it2->first] += it2->second;
            }
        }
    }
    // Local cache_counter_table free.
    for ( it = cache_file_info->cache_counter_table->begin(); it != cache_file_info->cache_counter_table->end(); ++it) {
        delete it->second->cache_page_usage;
        free(it->second);
    }
    delete cache_file_info->cache_counter_table;
    #endif
    cache_add_stat(cache_info->cache_stat_table[0][file], cache_file_info->cache_stat);
    delete cache_file_info->cache_stat;
    #endif
}

static void cache_deregister_lock(Cache_info *cache_info, std::string file, Cache_file_info *cache_file_info) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    cache_deregister(cache_info, file, cache_file_info);

}


static void cache_init(Cache_info *cache_info) {
    cache_info->cache_table = new std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*>;
    cache_info->cache_update_table = new std::map<std::string, std::set<off_t>*>;
    cache_info->cache_mutex_table = new std::map<std::string, tl::mutex*>;
    cache_info->cache_mem_mutex_table = new std::map<std::string, tl::mutex*>;
    cache_info->cache_offset_list_table = new std::map<std::string, std::vector<off_t>*>;
    cache_info->register_table = new std::map<std::string, int>;
    cache_info->cache_block_reserve_table = new std::map<std::string, int>;
    cache_info->cache_mutex = new tl::mutex;
    cache_info->cache_timestamps = new std::map<std::string, double>;
    cache_info->fd_table = new std::map<std::string, int>;
    cache_info->cache_page_refcount_table = new std::map<std::string, std::map<off_t, int>*>;
    cache_info->cache_backup_memory_table = new std::map<std::string, std::vector<char*>*>;
    cache_info->file_size_table = new std::map<std::string, size_t>;
    cache_info->cache_page_written_table = new std::map<std::string, std::map<off_t, size_t>*>;

    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    cache_info->cache_counter_table = new std::map<std::string, std::map<int, Cache_counter*>*>;
    cache_info->cache_counter_sum = new std::map<int, Cache_counter*>;
    #endif

    cache_info->cache_stat_table = new std::map<std::string, Cache_stat*>;
    cache_info->cache_stat_sum = (Cache_stat*) calloc(1, sizeof(Cache_stat));
    cache_info->max_request = -1;
    cache_info->min_request = -1;
    #endif
    cache_info->init_timestamp = (double*) malloc(sizeof(double));
    cache_info->init_timestamp[0] = ABT_get_wtime();

    cache_info->cache_block_used = (int*) calloc(2, sizeof(int));
    cache_info->shutdown = cache_info->cache_block_used + 1;
}

static void cache_finalize(Cache_info *cache_info) {

    std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*>::iterator it;
    for ( it = cache_info->cache_table->begin(); it != cache_info->cache_table->end(); ++it ) {
        std::map<off_t, std::pair<uint64_t, char*>*>::iterator it2;
        for ( it2 = it->second->begin(); it2 != it->second->end(); ++it2 ) {
            free(it2->second->second);
            delete it2->second;
        }
        delete it->second;
    }
    std::map<std::string, std::set<off_t>*>::iterator it3;
    for (it3 = cache_info->cache_update_table->begin(); it3 != cache_info->cache_update_table->end(); ++it3) {
        delete it3->second;
    }
    std::map<std::string, tl::mutex*>::iterator it4;
    for (it4 = cache_info->cache_mutex_table->begin(); it4 != cache_info->cache_mutex_table->end(); ++it4) {
        delete it4->second;
    }
    for (it4 = cache_info->cache_mem_mutex_table->begin(); it4 != cache_info->cache_mem_mutex_table->end(); ++it4) {
        delete it4->second;
    }
    std::map<std::string, std::vector<off_t>*>::iterator it5;
    for (it5 = cache_info->cache_offset_list_table->begin(); it5 != cache_info->cache_offset_list_table->end(); ++it5) {
        delete it5->second;
    }

    std::map<std::string, std::map<off_t, int>*>::iterator it10;
    for (it10 = cache_info->cache_page_refcount_table->begin(); it10 != cache_info->cache_page_refcount_table->end(); ++it10) {
        delete it10->second;
    }

    std::map<std::string, std::vector<char*>*>::iterator it11;
    std::vector<char*>::iterator it12;
    for (it11 = cache_info->cache_backup_memory_table->begin(); it11 != cache_info->cache_backup_memory_table->end(); ++it11) {
        for ( it12 = it11->second->begin(); it12 != it11->second->end(); ++it12 ) {
            free(*it12);
        }
        delete it11->second;
    }

    std::map<std::string, std::map<off_t, size_t>*>::iterator it13;
    for (it13 = cache_info->cache_page_written_table->begin(); it13 != cache_info->cache_page_written_table->end(); ++it13) {
        delete it13->second;
    }
    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    std::map<std::string, std::map<int, Cache_counter*>*>::iterator it6;
    for (it6 = cache_info->cache_counter_table->begin(); it6 != cache_info->cache_counter_table->end(); ++it6) {
        std::map<int, Cache_counter*>::iterator it7;
        for ( it7 = it6->second->begin(); it7 != it6->second->end(); ++it7 ) {
            delete it7->second->cache_page_usage;
            free(it7->second);
        }

        delete it6->second;
    }

    std::map<int, Cache_counter*>::iterator it9;
    for (it9 = cache_info->cache_counter_sum->begin(); it9 != cache_info->cache_counter_sum->end(); ++it9) {
        free(it9->second);
    }
    #endif

    std::map<std::string, Cache_stat*>::iterator it8;
    for (it8 = cache_info->cache_stat_table->begin(); it8 != cache_info->cache_stat_table->end(); ++it8) {
        free(it8->second);
    }

    #endif

    delete cache_info->cache_mem_mutex_table;
    delete cache_info->cache_table;
    delete cache_info->cache_update_table;
    delete cache_info->cache_mutex_table;
    delete cache_info->cache_offset_list_table;
    delete cache_info->cache_mutex;
    delete cache_info->register_table;
    delete cache_info->cache_block_reserve_table;
    delete cache_info->cache_timestamps;
    delete cache_info->fd_table;
    delete cache_info->cache_page_refcount_table;
    delete cache_info->cache_page_written_table;
    delete cache_info->file_size_table;
    delete cache_info->cache_backup_memory_table;

    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    delete cache_info->cache_counter_table;
    delete cache_info->cache_counter_sum;
    #endif

    delete cache_info->cache_stat_table;
    free(cache_info->cache_stat_sum);
    #endif

    free(cache_info->init_timestamp);
    free(cache_info->cache_block_used);
}

/*
 * This function is not thread-safe, so it should be called by a thread-safe function.
 * Flush many cache blocks into memory.
*/
static void cache_flush_array(Cache_file_info *cache_file_info, const std::vector<off_t> *cache_offsets, const int clean_memory) {
    //printf("reached flush array\n");
    unsigned i;
    off_t cache_offset;

    std::vector<abt_io_op_t*> *write_ops = new std::vector<abt_io_op_t*>;
    abt_io_op_t * write_op;
    std::vector<abt_io_op_t*>::iterator it2;

    #if BENVOLIO_CACHE_STATISTICS == 1
    double time = ABT_get_wtime();
    #endif

    for ( i = 0; i < cache_offsets->size(); ++i ) {
        cache_offset = cache_offsets[0][i];
        if (cache_file_info->cache_update_list->find(cache_offset) != cache_file_info->cache_update_list->end()) {
            // Time interval counter.
            #if BENVOLIO_CACHE_STATISTICS == 1
            cache_file_info->cache_stat->cache_counter.cache_block_flush_count++;
            #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
            int t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
            if (cache_file_info->cache_counter_table->find(t_index) != cache_file_info->cache_counter_table->end() ) {
                cache_file_info->cache_counter_table[0][t_index]->cache_block_flush_count++;
            } else {
                cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
                cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
                cache_file_info->cache_counter_table[0][t_index]->cache_block_flush_count++;
            }
            #endif
            #endif
            //write-back when the cache page is dirty. Maybe we can try to prioritize pages untouched or almost finished?

            ssize_t ret;
            write_op = abt_io_pwrite_nb(cache_file_info->abt_id, cache_file_info->fd, cache_file_info->cache_table[0][cache_offset]->second, cache_file_info->cache_table[0][cache_offset]->first, cache_offset, &ret );

            write_ops->push_back(write_op);

        }
    }

    if (write_ops->size() ) {
        it2 = write_ops->begin();
    }

    for ( i = 0; i < cache_offsets->size(); ++i ) {
        cache_offset = cache_offsets[0][i];

        if (cache_file_info->cache_update_list->find(cache_offset) != cache_file_info->cache_update_list->end()) {
            cache_file_info->cache_update_list->erase(cache_offset);

            write_op = *it2;
            abt_io_op_wait(write_op);
            abt_io_op_free(write_op);
            it2++;
        }

        //Remove memory and table entry
        //free(cache_file_info->cache_table[0][cache_offset]->second);

        cache_file_info->cache_page_written_table->erase(cache_offset);
        if (clean_memory) {
            cache_free(cache_file_info, cache_file_info->cache_table[0][cache_offset]->second);

            delete cache_file_info->cache_table[0][cache_offset];
            cache_file_info->cache_table->erase(cache_offset);

            cache_file_info->cache_page_refcount_table->erase(cache_offset);

            std::vector<off_t>::iterator it3 = std::find(cache_file_info->cache_offset_list->begin(), cache_file_info->cache_offset_list->end(), cache_offset);
            cache_file_info->cache_offset_list->erase(it3);
        }
    }
    delete write_ops;

    #if BENVOLIO_CACHE_STATISTICS == 1
    cache_file_info->cache_stat->flush_time += ABT_get_wtime() - time;
    #endif
}

static void cache_count_request_pages(Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size, std::set<off_t> *pages) {
    uint64_t my_provider;
    off_t cache_offset, block_index, subblock_index;
    off_t cache_start;
    int stripe_count, stripe_size;
    size_t i, cache_size, cache_blocks;
    stripe_size = cache_file_info->stripe_size;
    stripe_count = cache_file_info->stripe_count;

    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }

    // We can assume that non-contiguous block does not span across two Lustre stripes because the remote client should have called ad_lustre_calc_my_req kind of function to chop this kind of requests. However, it does not harm to be extra careful here.
    my_provider = ((file_start % (stripe_size * stripe_count)) / stripe_size);
    //block_index is from 0, 1, 2, 3..... We can multiply this by a whole lustre stripe to work out the beginning of caches.
    block_index = file_start / (stripe_size * stripe_count);
    subblock_index = (file_start % stripe_size) / cache_size;
    cache_start = (file_start % stripe_size) % cache_size;
    // Simplified from cache_blocks = ((file_start + file_size-1) %stripe_size + 1 + cache_size - 1) / cache_size;
    cache_blocks = ((file_start + file_size - 1) % stripe_size + cache_size) / cache_size;
    for ( i = subblock_index; i < cache_blocks; ++i ) {
        cache_offset = block_index * stripe_size * stripe_count + my_provider * stripe_size + i * cache_size;
        //printf("touching cache_offset = %llu\n", (long long unsigned) cache_offset);
        /* Sometimes the beginning of a cache block can go beyond the file range. For read, we can just terminate the caching process.*/
        if (cache_file_info->io_type == BENVOLIO_CACHE_READ && cache_offset + cache_start >= cache_file_info->file_size) {
            break;
        }
        pages->insert(cache_offset);
    }
}

static void cache_count_request_extra_pages(Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size, std::set<off_t> *pages) {
    uint64_t my_provider;
    off_t cache_offset, block_index, subblock_index;
    off_t cache_start;
    int stripe_count, stripe_size;
    size_t i, cache_size, cache_blocks;
    stripe_size = cache_file_info->stripe_size;
    stripe_count = cache_file_info->stripe_count;

    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }

    // We can assume that non-contiguous block does not span across two Lustre stripes because the remote client should have called ad_lustre_calc_my_req kind of function to chop this kind of requests. However, it does not harm to be extra careful here.
    my_provider = ((file_start % (stripe_size * stripe_count)) / stripe_size);
    //block_index is from 0, 1, 2, 3..... We can multiply this by a whole lustre stripe to work out the beginning of caches.
    block_index = file_start / (stripe_size * stripe_count);
    subblock_index = (file_start % stripe_size) / cache_size;
    cache_start = (file_start % stripe_size) % cache_size;
    // Simplified from cache_blocks = ((file_start + file_size-1) %stripe_size + 1 + cache_size - 1) / cache_size;
    cache_blocks = ((file_start + file_size - 1) % stripe_size + cache_size) / cache_size;
    for ( i = subblock_index; i < cache_blocks; ++i ) {
        cache_offset = block_index * stripe_size * stripe_count + my_provider * stripe_size + i * cache_size;
        //printf("touching cache_offset = %llu\n", (long long unsigned) cache_offset);
        /* Sometimes the beginning of a cache block can go beyond the file range. For read, we can just terminate the caching process.*/
        if (cache_file_info->io_type == BENVOLIO_CACHE_READ && cache_offset + cache_start >= cache_file_info->file_size) {
            break;
        }
        if (cache_file_info->cache_table->find(cache_offset) == cache_file_info->cache_table->end() ) {
            pages->insert(cache_offset);
        }
    }
}

static size_t cache_count_requests_pages(Cache_file_info *cache_file_info, const std::vector<off_t> &file_starts, const std::vector<uint64_t> &file_sizes) {
    std::set<off_t>* pages = new std::set<off_t>;
    size_t i;
    for ( i = 0; i < file_starts.size(); ++i ) {
        cache_count_request_extra_pages(cache_file_info, file_starts[i], file_sizes[i], pages);
    }
    size_t result = pages->size();
    delete pages;
    return result;
}

/**
 * Actively initilize all pages for a request.
*/
static void cache_allocate_memory(Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size) {
    uint64_t my_provider;
    off_t cache_offset, block_index, subblock_index;
    off_t cache_start, flush_offset;
    int stripe_count, stripe_size;
    size_t actual, i, cache_size, cache_size2, cache_blocks;
    stripe_size = cache_file_info->stripe_size;
    stripe_count = cache_file_info->stripe_count;
    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    int t_index;
    #endif
    double time = ABT_get_wtime();
    #endif

    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }

    // We can assume that non-contiguous block does not span across two Lustre stripes because the remote client should have called ad_lustre_calc_my_req kind of function to chop this kind of requests. However, it does not harm to be extra careful here.
    my_provider = ((file_start % (stripe_size * stripe_count)) / stripe_size);
    //block_index is from 0, 1, 2, 3..... We can multiply this by a whole lustre stripe to work out the beginning of caches.
    block_index = file_start / (stripe_size * stripe_count);
    subblock_index = (file_start % stripe_size) / cache_size;
    cache_start = (file_start % stripe_size) % cache_size;
    // Simplified from cache_blocks = ((file_start + file_size-1) %stripe_size + 1 + cache_size - 1) / cache_size;
    cache_blocks = ((file_start + file_size - 1) % stripe_size + cache_size) / cache_size;

    for ( i = subblock_index; i < cache_blocks; ++i ) {
        cache_offset = block_index * stripe_size * stripe_count + my_provider * stripe_size + i * cache_size;
        /* Sometimes the beginning of a cache block can go beyond the file range. For read, we can just terminate the caching process.*/
        if (cache_file_info->io_type == BENVOLIO_CACHE_READ && cache_offset + cache_start >= cache_file_info->file_size) {
            break;
        }

        std::vector<off_t>::iterator it = std::find(cache_file_info->cache_offset_list->begin(), cache_file_info->cache_offset_list->end(), cache_offset);
        if (it != cache_file_info->cache_offset_list->end()) {
            // Old page? move it to the back of the list.
            cache_file_info->cache_offset_list->erase(it);
        }
        cache_file_info->cache_offset_list->push_back(cache_offset);

        if (cache_file_info->io_type == BENVOLIO_CACHE_WRITE) {
            cache_file_info->cache_update_list->insert(cache_offset);
        }
        if ( cache_file_info->cache_table->find(cache_offset) == cache_file_info->cache_table->end() ) {
            #if BENVOLIO_CACHE_STATISTICS == 1
            cache_file_info->cache_stat->cache_counter.cache_page_fetch_count++;

            #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
            t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
            if (cache_file_info->cache_counter_table->find(t_index) == cache_file_info->cache_counter_table->end() ) {
                cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
                cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;                
            }
            cache_file_info->cache_counter_table[0][t_index]->cache_page_fetch_count++;
            #endif

            #endif


            /* This is the first time this cache block is accessed, we allocate memory and fetch the entire stripe to our memory*/
            cache_file_info->cache_table[0][cache_offset] = new std::pair<uint64_t, char*>;
            // We prevent caching subblock region that can exceed current stripe.
            cache_size2 = MIN(cache_size, stripe_size - cache_offset % stripe_size);

            // This region is the maximum possible cache, we may not necessarily use all of it, but we can adjust size later without realloc.
            cache_file_info->cache_table[0][cache_offset]->second = cache_malloc(cache_file_info, cache_size);
            //Register cache page lock
            cache_file_info->cache_page_refcount_table[0][cache_offset] = 0;

            //printf("ssg_rank = %d creating cache offset = %llu of size %ld\n", cache_file_info->ssg_rank, (long long unsigned) cache_offset, cache_size2);
            //printf("ssg_rank = %d creating cache offset = %llu\n", cache_file_info->ssg_rank, (long long unsigned) cache_offset);

            // The last stripe does not necessarily have stripe size number of bytes, so we need to store the actual number of bytes cached (so we can do write-back later without appending garbage data). Also, if write operation covers an entire cache page, we should just skip the read operation.

            if (cache_file_info->io_type == BENVOLIO_CACHE_READ || ( (i == subblock_index && cache_offset < file_start) || file_start + file_size < cache_offset + cache_size2 ) ) {
                actual = abt_io_pread(cache_file_info->abt_id, cache_file_info->fd, cache_file_info->cache_table[0][cache_offset]->second, cache_size2, cache_offset);
                //printf("reading %ld bytes to offset %ld\n", actual, cache_offset);
            } else {
                actual = 0;
            }
            // There is one exception, what if the file does not exist and a write operation is creating it? We should give the cache enough region to cover the write operation despite the fact that the read operation responded insufficient bytes.
            if (cache_file_info->io_type == BENVOLIO_CACHE_WRITE) {
                // Last block has "remainder" number of bytes, otherwise we can have full cache size.
                if ( i == cache_blocks - 1 ) {
                    cache_file_info->cache_table[0][cache_offset]->first = (((file_start + file_size - 1) % stripe_size) % cache_size) + 1;
                } else {
                    cache_file_info->cache_table[0][cache_offset]->first = cache_size2;
                }
                // Number of bytes written to this page.
                if ( file_start <= cache_offset ) {
                     // start offset is in the left of this cache page, so we can reuse the cache page size just created.
                    cache_file_info->cache_page_written_table[0][cache_offset] = cache_file_info->cache_table[0][cache_offset]->first;               
                } else if ( file_start + file_size >= cache_offset + cache_size2 ) {
                    // start offset is in middle of this page, but the tail is beyond this page, we cut the head.
                    cache_file_info->cache_page_written_table[0][cache_offset] = cache_size2 - file_start % stripe_size % cache_size;
                } else {
                    // request is a subset of the cache page, we just record the request size here.
                    cache_file_info->cache_page_written_table[0][cache_offset] = file_size;
                }

            } else if (cache_file_info->io_type == BENVOLIO_CACHE_READ) {
                cache_file_info->cache_table[0][cache_offset]->first = (uint64_t) actual;
            }

        } else if (cache_file_info->io_type == BENVOLIO_CACHE_WRITE) {
            // Time interval counter initialization
            #if BENVOLIO_CACHE_STATISTICS == 1

            #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
            t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
            if (cache_file_info->cache_counter_table->find(t_index) == cache_file_info->cache_counter_table->end() ) {
                cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*)calloc(1, sizeof(Cache_counter));
                cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
            }
            cache_file_info->cache_counter_table[0][t_index]->cache_page_hit_count++;
            #endif

            cache_file_info->cache_stat->cache_counter.cache_page_hit_count++;
            #endif


            // We may need to enlarge the cache array size in the last block of this stripe when a new write operation comes in because the new offset can exceed the cache domain.
            cache_size2 = MIN(cache_size, stripe_size - i * cache_size);
            if (file_start + file_size >= cache_offset + cache_size2) {
                // Largest possible cache we can have because the file range has been extended
                cache_file_info->cache_table[0][cache_offset]->first = cache_size2;
            } else if (cache_file_info->cache_table[0][cache_offset]->first < (((file_start + file_size - 1) % stripe_size) % cache_size) + 1) {
                // Enlarge the cache accordingly if necessary
                cache_file_info->cache_table[0][cache_offset]->first = (((file_start + file_size - 1) % stripe_size) % cache_size) + 1;
            }
            // Number of bytes written to this page.
            if ( file_start <= cache_offset ) {
                // start offset is in the left of this cache page.
                if ( file_start + file_size >= cache_offset + cache_size2 ) {
                    // tail is outside of this page.
                    cache_file_info->cache_page_written_table[0][cache_offset] += cache_size2;
                } else {
                    // tail is inside of this page.
                    cache_file_info->cache_page_written_table[0][cache_offset] += file_size + file_start - cache_offset;
                }
            } else if ( file_start + file_size >= cache_offset + cache_size2 ) {
                // start offset is in middle of this page, but the tail is beyond this page, we cut the head.
                cache_file_info->cache_page_written_table[0][cache_offset] += cache_size2 - file_start % stripe_size % cache_size;
            } else {
                // request is a subset of the cache page, we just record the request size here.
                cache_file_info->cache_page_written_table[0][cache_offset] += file_size;
            }
        }
        // Not in the previous condition because the cache size may change, we want the latest cache page size updated here.
        std::pair<uint64_t, char*> v;
        v.first = cache_file_info->cache_table[0][cache_offset]->first;
        v.second = cache_file_info->cache_table[0][cache_offset]->second;
        cache_file_info->cache_page_table[0][cache_offset] = v;
    }
    #if BENVOLIO_CACHE_STATISTICS == 1
    cache_file_info->cache_stat->cache_fetch_time += ABT_get_wtime() - time;
    #endif
}

static size_t cache_match_lock_free(char* local_buf, Cache_file_info *cache_file_info, const off_t file_start, const uint64_t file_size) {
/*
    #if BENVOLIO_CACHE_STATISTICS == 1
    std::lock_guard<tl::mutex> guard(*(cache_file_info->cache_mutex));
    #endif
*/
    uint64_t my_provider;
    off_t cache_offset, block_index, subblock_index;
    off_t cache_start;
    size_t actual, i, cache_size, cache_size2, cache_blocks;
    uint64_t remaining_file_size = file_size, cache_page_size;
    int stripe_count, stripe_size;
    #if BENVOLIO_CACHE_STATISTICS == 1

    #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
    int t_index;
    #endif
    double time, total_time2 = ABT_get_wtime();
    #endif
    stripe_size = cache_file_info->stripe_size;
    stripe_count = cache_file_info->stripe_count;

    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }

    // We can assume that non-contiguous block does not span across two Lustre stripes because the remote client should have called ad_lustre_calc_my_req kind of function to chop this kind of requests. However, it does not harm to be extra careful here.
    my_provider = ((file_start % (stripe_size * stripe_count)) / stripe_size);
    //block_index is from 0, 1, 2, 3..... We can multiply this by a whole lustre stripe to work out the beginning of caches.
    block_index = file_start / (stripe_size * stripe_count);
    subblock_index = (file_start % stripe_size) / cache_size;
    cache_start = (file_start % stripe_size) % cache_size;
    // Simplified from cache_blocks = ((file_start + file_size-1) %stripe_size + 1 + cache_size - 1) / cache_size;
    cache_blocks = ((file_start + file_size - 1) % stripe_size + cache_size) / cache_size;

    //printf("cache fetch subblocks_index = %llu, cache_blocks = %llu\n", (long long unsigned) subblock_index, (long long unsigned) cache_blocks );
    //printf("ssg_rank %d file_start=%llu, file size=%llu, cache_start = %llu\n",cache_file_info->ssg_rank, file_start, file_size, (long long unsigned) cache_start);
    for ( i = subblock_index; i < cache_blocks; ++i ) {
        cache_offset = block_index * stripe_size * stripe_count + my_provider * stripe_size + i * cache_size;
        /* Sometimes the beginning of a cache block can go beyond the file range. For read, we can just terminate the caching process.*/
        if (cache_file_info->io_type == BENVOLIO_CACHE_READ && cache_offset + cache_start >= cache_file_info->file_size) {
            //printf("ssg_rank = %d, cache offset %llu is beyond file size %llu\n", cache_file_info->ssg_rank, (long long unsigned )cache_offset, (long long unsigned) cache_file_info->file_size);
            break;
        }
        // It is impossible for this branch to have cache pages uninitialized at this point.
        cache_page_size = cache_file_info->cache_page_table[0][cache_offset].first;
        #if BENVOLIO_CACHE_STATISTICS == 1
        ABT_mutex_lock(cache_file_info->thread_mutex[0]);
        #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
        t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
        if (cache_file_info->cache_counter_table->find(t_index) == cache_file_info->cache_counter_table->end() ) {
            cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*)calloc(1, sizeof(Cache_counter));
            cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
        }
        #endif
        cache_file_info->cache_stat->cache_counter.cache_page_hit_count++;
        ABT_mutex_unlock(cache_file_info->thread_mutex[0]);
        #endif

        // Start to match data from cache.
        #if BENVOLIO_CACHE_STATISTICS == 1
        time = ABT_get_wtime();
        #endif
        if ( remaining_file_size > cache_page_size - cache_start ) {
            //printf("ssg_rank %d cache match regular block %llu\n", cache_file_info->ssg_rank, (long long unsigned) cache_offset);
            /* We are not at the last block yet */
            actual = cache_page_size - cache_start;
            //printf("actual = %llu, cache_start = %llu, cache_size = %llu\n", (long long unsigned) actual, (long long unsigned) cache_start, (long long unsigned) cache_file_info->cache_page_size[0][cache_offset]);
            if ( cache_file_info->io_type == BENVOLIO_CACHE_WRITE ) {
                //Copy from cache buffer to user buffer.
                memcpy(cache_file_info->cache_page_table[0][cache_offset].second + cache_start, local_buf, actual);
                //Need to mark this cache block has been updated.
                //cache_file_info->cache_update_list->insert(cache_offset);
                #if BENVOLIO_CACHE_STATISTICS == 1
                #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
                ABT_mutex_lock(cache_file_info->thread_mutex[0]);
                t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
                if (cache_file_info->cache_counter_table->find(t_index) != cache_file_info->cache_counter_table->end() ) {
                    if (cache_file_info->cache_counter_table[0][t_index]->cache_page_usage->find(cache_offset) != cache_file_info->cache_counter_table[0][t_index]->cache_page_usage->end()) {
                        cache_file_info->cache_counter_table[0][t_index]->cache_page_usage[0][cache_offset] += actual;
                    } else {
                        cache_file_info->cache_counter_table[0][t_index]->cache_page_usage[0][cache_offset] = actual;
                    }
                } else {
                    cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
                    cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
                    cache_file_info->cache_counter_table[0][t_index]->cache_page_usage[0][cache_offset] = actual;
                }
                ABT_mutex_unlock(cache_file_info->thread_mutex[0]);
                #endif
                #endif
            } else if (cache_file_info->io_type == BENVOLIO_CACHE_READ){
                //printf("ssg_rank %d offset %llu size %llu, cache_offset = %llu copying contiguous %llu number of bytes cache_start = %llu\n", cache_file_info->ssg_rank, (long long unsigned) file_start, (long long unsigned)file_size, cache_offset, (long long unsigned) actual, (long long unsigned) cache_start);
                memcpy(local_buf, cache_file_info->cache_page_table[0][cache_offset].second + cache_start, actual);
            }

            remaining_file_size -= actual;
            cache_start = 0;
            local_buf += actual;
        } else {
            //printf("ssg_rank %d offset %llu size %llu, cache_offset = %llu copying remaining %llu number of bytes cache_start = %llu\n", cache_file_info->ssg_rank, (long long unsigned) file_start, (long long unsigned)file_size, cache_offset, (long long unsigned) remaining_file_size, (long long unsigned) cache_start);
            //In some scenarios, the cache_start can beyond the cache page (bounded by the real file size), which leads to undefined behavior for read. For write, this should never happen because we should have reserved enough page size for this request.
            /* Last block, we may need to write a partial page. */
            if ( cache_file_info->io_type == BENVOLIO_CACHE_WRITE ) {
                // Copy from buffer to cache.
                memcpy(cache_file_info->cache_page_table[0][cache_offset].second + cache_start, local_buf, remaining_file_size);
                //Need to mark this cache block has been updated.
                //cache_file_info->cache_update_list->insert(cache_offset);

                // Log updates in the time interval.
                #if BENVOLIO_CACHE_STATISTICS == 1
                #if BENVOLIO_CACHE_STATISTICS_DETAILED == 1
                ABT_mutex_lock(cache_file_info->thread_mutex[0]);
                t_index = CACULATE_TIMESTAMP(ABT_get_wtime(), cache_file_info->init_timestamp[0]);
                if (cache_file_info->cache_counter_table->find(t_index) != cache_file_info->cache_counter_table->end() ) {
                    if (cache_file_info->cache_counter_table[0][t_index]->cache_page_usage->find(cache_offset) != cache_file_info->cache_counter_table[0][t_index]->cache_page_usage->end()) {
                        cache_file_info->cache_counter_table[0][t_index]->cache_page_usage[0][cache_offset] += remaining_file_size;
                    } else {
                        cache_file_info->cache_counter_table[0][t_index]->cache_page_usage[0][cache_offset] = remaining_file_size;
                    }
                } else {
                    cache_file_info->cache_counter_table[0][t_index] = (Cache_counter*) calloc(1, sizeof(Cache_counter));
                    cache_file_info->cache_counter_table[0][t_index]->cache_page_usage = new std::map<off_t, uint64_t>;
                    cache_file_info->cache_counter_table[0][t_index]->cache_page_usage[0][cache_offset] = remaining_file_size;
                }
                ABT_mutex_unlock(cache_file_info->thread_mutex[0]);
                #endif
                #endif

                remaining_file_size = 0;
            } else if (cache_file_info->io_type == BENVOLIO_CACHE_READ && cache_start < cache_page_size){
                //printf("ssg_rank %d offset %llu size %llu, cache_offset = %llu copying remaining %llu number of bytes cache_start = %llu\n", cache_file_info->ssg_rank, (long long unsigned) file_start, (long long unsigned)file_size, cache_offset, (long long unsigned) remaining_file_size, (long long unsigned) cache_start);
                actual = MIN(cache_page_size - cache_start, remaining_file_size);
                memcpy(local_buf, cache_file_info->cache_page_table[0][cache_offset].second + cache_start, actual);
                remaining_file_size -= actual;
            }
            #if BENVOLIO_CACHE_STATISTICS == 1
            ABT_mutex_lock(cache_file_info->thread_mutex[0]);
            cache_file_info->cache_stat->memcpy_time += ABT_get_wtime() - time;
            ABT_mutex_unlock(cache_file_info->thread_mutex[0]);
            #endif
            break;
        }
        #if BENVOLIO_CACHE_STATISTICS == 1
        ABT_mutex_lock(cache_file_info->thread_mutex[0]);
        cache_file_info->cache_stat->memcpy_time += ABT_get_wtime() - time;
        ABT_mutex_unlock(cache_file_info->thread_mutex[0]);
        #endif
    }
    #if BENVOLIO_CACHE_STATISTICS == 1
    ABT_mutex_lock(cache_file_info->thread_mutex[0]);
    cache_file_info->cache_stat->cache_match_time += time - total_time2;
    ABT_mutex_unlock(cache_file_info->thread_mutex[0]);
    #endif

    return file_size - remaining_file_size;
}

static int cache_shutdown_flag(Cache_info *cache_info) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    cache_info->shutdown[0] = 1;
    return 0;
}

static int cache_resource_manager(Cache_info *cache_info) {
    std::lock_guard<tl::mutex> guard(*(cache_info->cache_mutex));
    cache_flush_all(cache_info, 1);
    return cache_info->shutdown[0];
}

struct resource_manager_args {
    ABT_eventual eventual;
    Cache_info *cache_info;
    abt_io_instance_id abt_id;
    int ssg_rank;
};

static void cache_resource_manager(void *_args) {
    struct resource_manager_args *args = (struct resource_manager_args *)_args;
    int temp;
    while (1) {
        temp = cache_resource_manager(args->cache_info);
        //printf("ssg_rank %d, resource management check for temp = %d\n", args->ssg_rank, temp);
        if (temp){
            break;
        }
        sleep(BENVOLIO_CACHE_RESOURCE_CHECK_TIME);
    }
    ABT_eventual_set(args->eventual, NULL, 0);
    return;
}

#endif

#endif
