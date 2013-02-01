cascading-trace
===============

This is a class for instrumenting a cascading flow.  The idea is to concatenate an extra "hidden" field to each
tuple, then define how the values get merged during a group-by or join.

A concrete implementation is provided, which is used to trace inputs through the flow.  What happens is that a field
is added to each pipe, where the corresponding value is a bloom filter containing that tuple.  At the end of the flow,
these are adde together, and so the input files can be filtered yielding only those rows which were used in the 
creation of output rows.
