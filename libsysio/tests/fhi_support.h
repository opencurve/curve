extern int _test_fhi_check_handle(const char *path,
				  struct file_handle_info *handle,
				  size_t nbytes);
extern int _test_fhi_start(int *key,
			   const char *path,
			   struct file_handle_info *root);
extern int _test_fhi_find(struct file_handle_info *parent,
			  const char *path,
struct file_handle_info *fhi);
