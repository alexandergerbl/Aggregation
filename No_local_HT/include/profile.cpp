#include <iostream>
#include <cstring>
#include <string>
#include <sys/time.h>
#include <functional>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>
#include <linux/perf_event.h>
#include <asm/unistd.h>
#include <vector>

class PerfEvent {
   struct perf_event_attr pe;
   int fd;
public:
   PerfEvent(uint64_t type, uint64_t event) {
      memset(&pe, 0, sizeof(struct perf_event_attr));
      pe.type = type;
      pe.size = sizeof(struct perf_event_attr);
      pe.config = event;
      pe.disabled = true;
      pe.exclude_kernel = true;
      pe.exclude_hv = true;
      fd = syscall( __NR_perf_event_open, &pe, 0, -1, -1, 0);
      if (fd < 0)
         fprintf(stderr, "Error opening leader %llx\n", pe.config);
      ioctl(fd, PERF_EVENT_IOC_RESET, 0);
      ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
   }
   ~PerfEvent() {
      close(fd);
   }
   uint64_t readCounter() {
      uint64_t count;
      if (read(fd, &count, sizeof(uint64_t))!=sizeof(uint64_t))
         fprintf(stderr, "Error reading counter\n");
      return count;
   }
};

class PerfEventMT {
   std::vector<struct perf_event_attr> pes;
   std::vector<int> fd;
   unsigned cpucount;

public:
   PerfEventMT(uint64_t type, uint64_t event) {
      cpucount=sysconf(_SC_NPROCESSORS_ONLN);
      pes.reserve(cpucount);
      fd.reserve(cpucount);

      for (unsigned i=0; i<cpucount; i++) {
         struct perf_event_attr& pe=pes[i];
         memset(&pe, 0, sizeof(struct perf_event_attr));
         pe.type = type;
         pe.size = sizeof(struct perf_event_attr);
         pe.config = event;
         pe.disabled = true;
         pe.exclude_kernel = true;
         pe.exclude_hv = true;

         fd[i] = syscall( __NR_perf_event_open, &pe, -1, i, -1, 0);

         if (fd[i] < 0) {
            fprintf(stderr, "Error opening leader %llx\n", pe.config);
         }
      }

      for (unsigned i=0; i<cpucount; i++) {
         ioctl(fd[i], PERF_EVENT_IOC_RESET, 0);
         ioctl(fd[i], PERF_EVENT_IOC_ENABLE, 0);
      }
   }

   ~PerfEventMT() {
      for (unsigned i=0; i<cpucount; i++)
         close(fd[i]);
   }

   uint64_t readCounter() {
      uint64_t sum=0;
      for (unsigned i=0; i<cpucount; i++) {
         uint64_t count;
         if (read(fd[i], &count, sizeof(uint64_t))!=sizeof(uint64_t))
            fprintf(stderr, "Error reading counter\n");
         sum+=count;
      }
      return sum;
   }
};


inline double gettime() {
  struct timeval now_tv;
  gettimeofday (&now_tv, NULL);
  return ((double)now_tv.tv_sec) + ((double)now_tv.tv_usec)/1000000.0;
}

size_t getCurrentRSS() {
   long rss = 0L;
   FILE* fp = NULL;
   if ( (fp = fopen( "/proc/self/statm", "r" )) == NULL )
      return (size_t)0L;/* Can't open? */
   if ( fscanf( fp, "%*s%ld", &rss ) != 1 ) {
      fclose( fp );
      return (size_t)0L;/* Can't read? */
   }
   fclose( fp );
   return (size_t)rss * (size_t)sysconf( _SC_PAGESIZE);
}

void timeAndProfile(std::string s,uint64_t n,std::function<void()> fn,bool mem=false) {
   if (getenv("WAIT")) {
      std::cout << s << " start ..." << std::endl;
      std::cin.ignore();
      double start = gettime();
      fn();
      double end = gettime();
      std::cout << s << " " << ((n/1e6)/(end-start)) << "M/s ..." << std::endl;
      std::cin.ignore();
      return;
   }

   uint64_t memStart=0;
   if (mem)
      memStart=getCurrentRSS();
   PerfEvent misses(PERF_TYPE_HARDWARE,PERF_COUNT_HW_CACHE_MISSES);
   PerfEvent instructions(PERF_TYPE_HARDWARE,PERF_COUNT_HW_INSTRUCTIONS);
   PerfEvent l1misses(PERF_TYPE_HW_CACHE,PERF_COUNT_HW_CACHE_L1D|(PERF_COUNT_HW_CACHE_OP_READ<<8)|(PERF_COUNT_HW_CACHE_RESULT_MISS<<16));
   PerfEvent bmiss(PERF_TYPE_HARDWARE,PERF_COUNT_HW_BRANCH_MISSES);
   double start = gettime();
   fn();
   double end = gettime();
   std::cout << s << " " << ((n/1e6)/(end-start)) << "M/s " << (misses.readCounter()/(float)n) << " misses " << (l1misses.readCounter()/(float)n) << " L1misses " << (instructions.readCounter()/(float)n) << " instructions " << (bmiss.readCounter()/(float)n) << " bmisses ";
   if (mem)
      std::cout << (getCurrentRSS()-memStart)/(1024.0*1024) << "MB";
   std::cout << std::endl;
}

void timeAndProfileMT(std::string s,uint64_t n,std::function<void()> fn,bool mem=false) {
   if (getenv("WAIT")) {
      std::cout << s << " start ..." << std::endl;
      std::cin.ignore();
      double start = gettime();
      fn();
      double end = gettime();
      std::cout << s << " " << ((n/1e6)/(end-start)) << "M/s ..." << std::endl;
      std::cin.ignore();
      return;
   }

   uint64_t memStart=0;
   if (mem)
      memStart=getCurrentRSS();
   PerfEventMT misses(PERF_TYPE_HARDWARE,PERF_COUNT_HW_CACHE_MISSES);
   PerfEventMT instructions(PERF_TYPE_HARDWARE,PERF_COUNT_HW_INSTRUCTIONS);
   PerfEventMT l1misses(PERF_TYPE_HW_CACHE,PERF_COUNT_HW_CACHE_L1D|(PERF_COUNT_HW_CACHE_OP_READ<<8)|(PERF_COUNT_HW_CACHE_RESULT_MISS<<16));
   PerfEventMT bmiss(PERF_TYPE_HARDWARE,PERF_COUNT_HW_BRANCH_MISSES);
   double start = gettime();
   fn();
   double end = gettime();
   std::cout << s << " " << ((n/1e6)/(end-start)) << "M/s " << (misses.readCounter()/(float)n) << " misses " << (l1misses.readCounter()/(float)n) << " L1misses " << (instructions.readCounter()/(float)n) << " instructions " << (bmiss.readCounter()/(float)n) << " bmisses ";
   if (mem)
      std::cout << (getCurrentRSS()-memStart)/(1024.0*1024) << "MB";
   std::cout << std::endl;
}


//ADDED to print gnuplot output
void timeAndProfileMT_OperationsPerSecond(int num_threads, uint64_t n,std::function<void()> fn,bool mem=false) {
   
   PerfEventMT instructions(PERF_TYPE_HARDWARE,PERF_COUNT_HW_INSTRUCTIONS);
   
   double start = gettime();
   fn();
   double end = gettime();
   std::cout << num_threads << "\t" << ((n/1e6)/(end-start)) << std::endl;
   
}
