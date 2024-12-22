namespace java com.example.pipeetoc

service PipeEtoCService {
    // 定义第一个 AckMessage 函数
    void AckMessage(1: i32 CloudFragmentId, 2: i32 SourceId);

    // 定义第二个 AckMessage 函数
    void AckMessageWithIndex(1: list<i32> newIndex, 2: i32 SourceId);
}
