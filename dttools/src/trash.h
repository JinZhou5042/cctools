#ifndef TRASH_H
#define TRASH_H

/** @file trash.h
Delete files and directories that may still be in use by moving them into a trash directory first. A rename takes
the name away at once, so a new object of the same name can be created right away, and the slow deletion of a large
tree can happen later, all at once with @ref trash_empty or in pieces with @ref trash_empty_budget.
*/

#include "budget.h"

/** Set the trash directory, and create it if needed.
@param trashdir The trash directory.
*/
void trash_setup( const char *trashdir );

/** Move a file or directory into the trash, and then empty the trash.
@param filename The file or directory to delete.
*/
void trash_file( const char *filename );

/** Move a file or directory into the trash without deleting anything. A later @ref trash_empty or
@ref trash_empty_budget deletes it.
@param filename The file or directory to delete.
*/
void trash_move( const char *filename );

/** Delete everything in the trash. */
void trash_empty();

/** Delete entries of the trash until the trash is empty or a budget expires. The budget is checked after each deleted
entry, so a call always makes progress, and a later call resumes where this one stopped. Entries that cannot be
deleted are reported and tried again after the next @ref trash_move or @ref trash_file.
@param b The budget that limits this call.
@return One when nothing is left to try, zero when entries remain for a later call.
*/
int trash_empty_budget( const struct budget *b );

#endif

/* vim: set noexpandtab tabstop=8: */
