#include "progress_bar.h"
#include "list.h"
#include "timestamp.h"
#include <unistd.h>
#include <stdio.h>

int main()
{
	uint64_t total = 100000;
	struct ProgressBarPart *part1 = progress_bar_part_create("step", total);
	struct ProgressBarPart *part2 = progress_bar_part_create("fetch", total);
	struct ProgressBarPart *part3 = progress_bar_part_create("commit", total);

	struct ProgressBar *bar = progress_bar_init("Compute");
	progress_bar_add_part(bar, part1);
	progress_bar_add_part(bar, part2);
	progress_bar_add_part(bar, part3);

	timestamp_t start_time = timestamp_get();
	for (uint64_t i = 0; i < total; i++) {
		progress_bar_advance_part_current(bar, part1, 1);
		progress_bar_advance_part_current(bar, part2, 1);
		progress_bar_advance_part_current(bar, part3, 1);
	}

	progress_bar_finish(bar);
	progress_bar_delete(bar);

	timestamp_t end_time = timestamp_get();
	printf("time taken: %ld\n", end_time - start_time);

	return 0;
}
