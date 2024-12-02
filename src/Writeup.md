# Writeup

## Describe any design decisions you made.
The width of a bucket is (max + 1-min)/buckets_. Such a calculation ensures that all buckets cover the entire range, even if max_ is not perfectly divisible by buckets_.

When bucket_width_ < 1.0, the partial coverage calculation (bucketFraction) is not applicable and unnecessary because the bucket width is already small enough to provide sufficient precision. This approach significantly improves efficiency and accuracy.
For bucket_width_ >= 1.0, it becomes necessary to calculate the portion of the current bucket that is not fully covered, which requires considering bucketFraction.
## Describe any missing or incomplete elements of your code.
We pass all test so that we are not missing or incomplete elements for our code.

## Describe how long you spent on the assignment, and whether there was anything you found particularly difficult or confusing.
I spent approximately 10 hour working on this assignment. The first challenge I faced was calculating bucket_width_. Initially, I forgot to include (max + 1) in the calculation. The second challenge was determining the value for bucketRangeEnd. I found this confusing, especially when considering examples like the ranges 1-10 and 11-20. Understanding how to properly calculate the end of each bucket range took some time and effort.
The third challenging part was handling the GT case. Initially, I failed this part because I only calculated the partially covered portion of the current bucket using bucketFraction (r−c)/w and overlooked the scenario where the bucket width (w) was too small. I later made adjustments to properly account for this edge case.
## If you collaborate with someone else, you have to describe how you split the workload.
I collaborated with Jasmine Dong on this project. Initially, we worked separately. Then, we had an discussion about the GT part and ultimately concluded that it was necessary to split it into two distinct scenarios. The decision was based on the observation that when the bucket width (w) is very small, calculating the partially covered portion using bucketFraction alone is sufficient. However, for larger bucket widths, we need to account for both the partially covered portion and the contribution from subsequent buckets. 
