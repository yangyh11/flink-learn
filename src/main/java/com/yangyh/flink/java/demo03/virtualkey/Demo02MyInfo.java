package com.yangyh.flink.java.demo03.virtualkey;

/**
 * @description:
 * @author: yangyh
 * @create: 2020-01-08 21:42
 */
public class Demo02MyInfo {

    private String word;
    private Integer count;

    public Demo02MyInfo() {
    }

    public Demo02MyInfo(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "MyInfo{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
