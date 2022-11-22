### 二分查找
#### 上下边界
*   模板1
<br>'''
int search(int left, int right) {<br>
    while (left < right) {<br>
        int mid = (left + right) >> 1;<br>
        if (check(mid)) {<br>
            right = mid;<br>
        } else {<br>
            left = mid + 1;<br>
        }
    }<br>
    return left;
}
'''
*   模板2
int search(int left, int right) {
    while (left < right) {
        int mid = (left + right + 1) >> 1;
        if (check(mid)) {
            left = mid;
        } else {
            right = mid - 1;
        }
    }
    return left;
}
*   思路
    1. 写出循环条件：while (left < right)，注意是 left < right，而非 left <= right；
    2. 循环体内，先无脑写出 mid = (left + right) >> 1；
    3. 根据具体题目，实现 check() 函数（有时很简单的逻辑，可以不定义 check），想一下究竟要用 right = mid（模板 1） 还是 left = mid（模板 2）；
        - 如果 right = mid，那么无脑写出 else 语句 left = mid + 1，并且不需要更改 mid 的计算，即保持 mid = (left + right) >> 1；
        - 如果 left = mid，那么无脑写出 else 语句 right = mid - 1，并且在 mid 计算时补充 +1，即 mid = (left + right + 1) >> 1。
    4. 循环结束时，left 与 right 相等。
    换言之 如果做二分，对于上下边界的查询，只需要注意到 当r=l+1 时，下一次循环是否会进入死循环。那么根据这个准则，来判断是mid = l+r+1 / 2还是 l+r / 2 