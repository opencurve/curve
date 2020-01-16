#ifndef SRC_PART2_NEBD_SERVER_H_
#define SRC_PART2_NEBD_SERVER_H_

namespace nebd {
namespace server {

class NebdServer {
 public:
    NebdServer() {}
    virtual ~NebdServer() {}
    int Init();
    int Run();
    int Fini();
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_NEBD_SERVER_H_
