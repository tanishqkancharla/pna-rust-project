My key-value database built for the [pingcap talent plan rust course](https://github.com/tanishqkancharla/talent-plan/blob/master/courses/rust/docs/lesson-plan.md).

For the most part, the architecture is very similar to [Bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf). There are no hint files and log files are currently stored in JSON (might eventually switch to `bincode`).
