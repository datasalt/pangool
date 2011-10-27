namespace java com.datasalt.pangolin.thrift.test

struct A {

	1: string id,
	2: string url
}

struct B {

	1: string url,
	2: string urlNorm
}

struct TestEntity {
	1: string str1,
	2: string str2,
	3: string str3,
	10: i32 number,
	20: i64 longNumber
}