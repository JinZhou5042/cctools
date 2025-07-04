#include "progress_bar.h"
#include <unistd.h>

int main()
{
	struct ProgressBar *bar = progress_bar_init("Generating plotting data", 100, 10);

	for (int i = 0; i <= 100; ++i) {
		progress_bar_update(bar, 1);
		usleep(50000);
	}

	progress_bar_finish(bar);

	return 0;
}
