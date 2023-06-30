function build() {
    rm -rf output # 清除上次编译结果
    mkdir -p output # 创建编译目录
    cp -r script/ output/
}

build
exit 0
