# googlemock 

## 简单示例

类的定义如下：

    class Turtle {
      ...
      virtual ~Turtle() {}
      virtual void PenUp() = 0;
      virtual void PenDown() = 0;
      virtual void Forward(int distance) = 0;
      virtual void Turn(int degrees) = 0;
      virtual void GoTo(int x, int y) = 0;
      virtual int GetX() const = 0;
      virtual int GetY() const = 0;
    };


### 创建mock类

创建mock的步骤如下：

1. 从Turtle派生一个类MockTurtle。
2. 使用Turtle的虚函数，计算它有多少参数。
3. 在子类的public：部分，写MOCK_METHODn（）; （或MOCK_CONST_METHODn（）;如果你是一个const方法），其中n是参数的数量;如果你计数错误，产生一个一个编译器错误。
4. 使用函数名作为宏的第一个参数，第二个参数是函数的类型。

例：

    #include "gmock/gmock.h"  // Brings in Google Mock.
    class MockTurtle : public Turtle {
     public:
      ...
      MOCK_METHOD0(PenUp, void());
      MOCK_METHOD0(PenDown, void());
      MOCK_METHOD1(Forward, void(int distance));
      MOCK_METHOD1(Turn, void(int degrees));
      MOCK_METHOD2(GoTo, void(int x, int y));
      MOCK_CONST_METHOD0(GetX, int());
      MOCK_CONST_METHOD0(GetY, int());
    };

### 在测试中使用mock类

在测试中使用mock类步骤如下：

1. 从测试命名空间导入Mock名称，以便可以使用它们。
2. 创建mock对象。
3. 指定期望（一个方法被调用多少次？有什么参数？它应该做什么等等）。
4. 执行一些使用mock的代码; 可以使用Google Test断言检查结果。如果一个mock方法被调用超过预期或错误的参数，会立即得到一个错误。
5. 当mock对象析构时，Google Mock将自动检查是否满足所有期望。

例：

    #include "path/to/mock-turtle.h"
    #include "gmock/gmock.h"
    #include "gtest/gtest.h"
    using ::testing::AtLeast;                     // #1

    TEST(PainterTest, CanDrawSomething) {
      MockTurtle turtle;                          // #2
      EXPECT_CALL(turtle, PenDown())              // #3
          .Times(AtLeast(1));

      Painter painter(&turtle);                   // #4

      EXPECT_TRUE(painter.DrawCircle(0, 0, 10));
    }                                             // #5

    int main(int argc, char** argv) {
      // The following line must be executed to initialize Google Mock
      // (and Google Test) before running the tests.
      ::testing::InitGoogleMock(&argc, argv);
      return RUN_ALL_TESTS();
    }

## 为一个类模板创建mock

    template <typename Elem>
    class StackInterface {
     public:
      ...
      virtual ~StackInterface();
      virtual int GetSize() const = 0;
      virtual void Push(const Elem& x) = 0;
    };

例：

    template <typename Elem>
    class MockStack : public StackInterface<Elem> {
     public:
      ...
      MOCK_CONST_METHOD0_T(GetSize, int());
      MOCK_METHOD1_T(Push, void(const Elem& x));
    };

区别在于在MOCK\_\* 之后加上了\_T后缀。

## 设置期望

期望的语法格式如下：

    EXPECT_CALL(mock_object, method(matcher1, matcher2, ...))
        .With(multi_argument_matcher)
        .Times(cardinality)
        .InSequence(sequences)
        .After(expectations)
        .WillOnce(action)
        .WillRepeatedly(action)
        .RetiresOnSaturation();

+ 第1行的mock_object就是你的Mock类的对象
+ 第1行的method(matcher1, matcher2, …)中的method就是你Mock类中的某个方法名，而matcher（匹配器）的意思是定义方法参数的类型。
+ 第3行的Times(cardinality)的意思是之前定义的method运行几次。
+ 第4行的InSequence(sequences)的意思是定义这个方法被执行顺序（优先级）。
+ 第6行WillOnce(action)是定义一次调用时所产生的行为，比如定义该方法返回怎么样的值等等。
+ 第7行WillRepeatedly(action)的意思是缺省/重复行为。

## Matchers（匹配器）

Matcher用于定义Mock类中的方法的形参的值（当然，如果你的方法不需要形参时，可以保持match为空）。 

Matcher可以在ON_CALL() or EXPECT_CALL()内部使用，也可以直接判断一个值，使用:

|EXPECT_THAT(value, matcher) | Asserts that value matches matcher. |
|:---------------------------|:----------------------|
|ASSERT_THAT(value, matcher) | The same as EXPECT_THAT(value, matcher), except that it generates a fatal failure. |

### 通配符

| _           |	可以代表任意类型      |
|:------------|:----------------------|
| A() or An() | 可以是type类型的任意值|

### 一般比较

| Eq(value) 或者 value	| argument == value，method中的形参必须是value |
|:----------------------|:----------------------|
| Ge(value)	            | argument >= value，method中的形参必须大于等于value |
| Gt(value)	            | argument > value |
| Le(value)	            | argument <= value |
| Lt(value)	            | argument < value |
| Ne(value)	            | argument != value |
| IsNull()	            | method的形参必须是NULL指针 |
| NotNull()	            | argument is a non-null pointer |
| Ref(variable)	        | 形参是variable的引用 |
| TypedEq(value)        | 形参的类型必须是type类型，而且值必须是value |

### 浮点数的比较

| DoubleEq(a_double)    | 形参是一个double类型，比如值近似于a_double，两个NaN是不相等的 |
|:----------------------|:----------------------|
| FloatEq(a_float)      | 同上，只不过类型是float |
| NanSensitiveDoubleEq(a_double) | 形参是一个double类型，比如值近似于a_double，两个NaN是相等的 |
| NanSensitiveFloatEq(a_float) | 同上，只不过形参是float |

### 字符串比较

| ContainsRegex(string)	| 形参匹配给定的正则表达式 |
|:----------------------|:----------------------|
| EndsWith(suffix)   	| 形参以suffix截尾 |
| HasSubstr(string)   	| 形参有string这个子串 |
| MatchesRegex(string)	| 从第一个字符到最后一个字符都完全匹配给定的正则表达式. |
| StartsWith(prefix)	| 形参以prefix开始 |
| StrCaseEq(string)	    | 参数等于string，并且忽略大小写 |
| StrCaseNe(string)	    | 参数不是string，并且忽略大小写 |
| StrEq(string)	        | 参数等于string |
| StrNe(string)	        | 参数不等于string |

### 容器比较

| Contains(e)	             | 在method的形参中，只要有其中一个元素等于e |
|:----------------------|:----------------------|
| Each(e)                    | 参数各个元素都等于e |
| ElementsAre(e0, e1, …, en) | 形参有n+1的元素，并且挨个匹配 |
| ElementsAreArray(array)    | 或者ElementsAreArray(array, count)	和ElementsAre()类似，除了预期值/匹配器来源于一个C风格数组 |
| ContainerEq(container)	 | 类型Eq(container)，就是输出结果有点不一样，这里输出结果会带上哪些个元素不被包含在另一个容器中 |
| Pointwise(m, container)	 |            |

所有这些Matchers都在命名空间::testing中，使用时需引入这一命名空间
例：

    using ::testing::Ge;
    using ::testing::NotNull;
    using ::testing::Return;
    ...
      EXPECT_CALL(foo, DoThis(Ge(5)))  // The argument must be >= 5.
          .WillOnce(Return('a'));
      EXPECT_CALL(foo, DoThat("Hello", NotNull()));
      // The second argument must not be NULL.



### 成员匹配器

| Field(&class::field, m)	      | argument.field (或 argument->field, 当argument是一个指针时)与匹配器m匹配, 这里的argument是一个class类的实例. |
|:----------------------|:----------------------|
| Key(e)	                      | 容器的key值符合e，e可以是一个值或者是一个Matcher |
| Pair(m1, m2)	                  | argument是一个std::pair，并且argument.first等于m1，argument.second等于m2. |
| Property(&class::property, m)	  | argument.property()(或argument->property(),当argument是一个指针时)与匹配器m匹配, 这里的argument是一个class类的实例. |

例：

    TEST(TestField, Simple) {
            MockFoo mockFoo;
            Bar bar;
            EXPECT_CALL(mockFoo, get(Field(&Bar::num, Ge(0)))).Times(1);
            mockFoo.get(bar);
    }

    int main(int argc, char** argv) {
            ::testing::InitGoogleMock(&argc, argv);
            return RUN_ALL_TESTS();
    }

### 匹配函数或函数对象的返回值

| ResultOf(f, m) | f(argument) 与匹配器m匹配, 这里的f是一个函数或函数对象.|
|:----------------------|:----------------------|

### 指针匹配器

| Pointee(m) | argument (不论是智能指针还是原始指针) 指向的值与匹配器m匹配. |
|:----------------------|:----------------------|
| WhenDynamicCastTo<T>(m) | 当argument通过dynamic_cast<T>()转换之后，与匹配器m匹配 |

### 多参数匹配器

同时比较两个参数x和y

| Eq()	| x == y |
|:------|:-------|
| Ge()	| x >= y |
| Gt()	| x > y |
| Le()	| x <= y |
| Lt()	| x < y |
| Ne()	| x != y |

| AllArgs(m) | 所有参数都匹配m |
|:-----------|:----------------|
|`Args<N1, N2, ..., Nk>(m)` | 其中的k个被选择的参数匹配m |

### 复合匹配器

| AllOf(m1, m2, …, mn)	| argument 匹配所有的匹配器m1到mn |
|:-----------|:----------------|
| AnyOf(m1, m2, …, mn)	| argument 至少匹配m1到mn中的一个 |
| Not(m)	            | argument 不与匹配器m匹配 |

## Cardinalities（基数）

基数用于Times()中来指定模拟函数将被调用多少次

| AnyNumber()	| 函数可以被调用任意次. |
|:----------------------|:----------------------|
| AtLeast(n)	| 预计至少调用n次. |
| AtMost(n)	    | 预计至多调用n次. |
| Between(m, n)	| 预计调用次数在m和n(包括n)之间. |
| Exactly(n)或n	| 预计精确调用n次. 特别是, 当n为0时,函数应该永远不被调用. |

## Actions（行为）

Actions（行为）用于指定Mock类的方法所期望模拟的行为：比如返回什么样的值、对引用、指针赋上怎么样个值，等等。 

### 返回值

| Return()	            | 让Mock方法返回一个void结果 |
|:----------------------|:----------------------|
| Return(value)	        | 返回值value |
| ReturnNull()	        | 返回一个NULL指针 |
| ReturnRef(variable)	| 返回variable的引用. |
| ReturnPointee(ptr)	| 返回一个指向ptr的指针 |

### Side Effects

Side Effects 指的是这个action还将对测试起的作用，比如给指针赋值之类的。

| Assign(&variable, value) | 将value分配给variable |
|:-------------------------|:----------------------|
| DeleteArg<N>()           | 删除第n个参数（从0开始）的值，必须是指针 |
| SaveArg<N>(pointer)      | 保存第n个参数（从0开始）的值到pointer所指向的位置 |
| SaveArgPointee<N>(pointer) | 保存第n个指针参数指向的值到pointer所指向的位置 |
| SetArgReferee<N>(value)    | 将value赋值给第n个参数所引用的值 |
| SetArgPointee<N>(value)    | 将value赋值给第n个指针参数所指向的值 |
| SetArrayArgument<N>(first, last)  | 将[first，last)范围内的指向的数据拷贝到数组的第N个位置开始的地址中 |    
| SetErrnoAndReturn(error, value)   | 设置errno为error，并且返回value |
| Throw(exception)                  | 抛出异常 |

### 使用函数或函数对象来作为action

| Invoke(f)	                                | 使用模拟函数的参数调用f, 这里的f可以是全局/静态函数或函数对象. |
|:-------------------------|:----------------------|
| Invoke(object_pointer, &class::method)	| 使用模拟函数的参数调用object_pointer对象的method方法. |

### 复合动作

| DoAll(a1, a2, …, an)	| 每次发动时执行a1到an的所有动作.  |
|:-------------------------|:----------------------|
| IgnoreResult(a)	    | 执行动作a并忽略它的返回值. a不能返回void. |

## 期望执行的顺序

一种是使用after子句

例：
    using ::testing::Expectation;
    ...
    Expectation init_x = EXPECT_CALL(foo, InitX());
    Expectation init_y = EXPECT_CALL(foo, InitY());
    EXPECT_CALL(foo, Bar())
        .After(init_x, init_y);

表示，bar()的函数调用必须在init_x和init_y之后发生，否则报错。

另外一种是使用Sequences

    using ::testing::Sequence;
    Sequence s1, s2;
    ...
    EXPECT_CALL(foo, Reset())
        .InSequence(s1, s2)
        .WillOnce(Return(true));
    EXPECT_CALL(foo, GetSize())
        .InSequence(s1)
        .WillOnce(Return(1));
    EXPECT_CALL(foo, Describe(A<const char*>()))
        .InSequence(s2)
        .WillOnce(Return("dummy"));

表示，Reset和GetSize在序列s1里，Reset和Describe在序列s2里，因此，要求Reset必须先于GetSize和Describe被调用，而GetSize和Describe之间可以任意顺序。

另一种用法，

    using ::testing::InSequence;
    {
      InSequence dummy;

      EXPECT_CALL(...)...;
      EXPECT_CALL(...)...;
      ...
      EXPECT_CALL(...)...;
    }

声明一个dummy的InSequence之后的EXPECT_CALL将必须按照调用的顺序执行，否则报错。


## Mock私有或保护函数

c++ 允许在子类中修改虚函数的访问级别
例：

    class Foo {
     public:
      ...
      virtual bool Transform(Gadget* g) = 0;

     protected:
      virtual void Resume();

     private:
      virtual int GetTimeOut();
    };

    class MockFoo : public Foo {
     public:
      ...
      MOCK_METHOD1(Transform, bool(Gadget* g));

      // The following must be in the public section, even though the
      // methods are protected or private in the base class.
      MOCK_METHOD0(Resume, void());
      MOCK_METHOD0(GetTimeOut, int());
    };

## Mock非虚函数

到目前为止，GMock给出的例子中，我们所mock的都是虚函数，那么非虚函数怎么办呢？

    // A simple packet stream class.  None of its members is virtual.
    class ConcretePacketStream {
     public:
      void AppendPacket(Packet* new_packet);
      const Packet* GetPacket(size_t packet_number) const;
      size_t NumberOfPackets() const;
      ...
    };

    // A mock packet stream class.  It inherits from no other, but defines
    // GetPacket() and NumberOfPackets().
    class MockPacketStream {
     public:
      MOCK_CONST_METHOD1(GetPacket, const Packet*(size_t packet_number));
      MOCK_CONST_METHOD0(NumberOfPackets, size_t());
      ...
    };

GMock官方文档中给出的方案是：
创建一个mock类，不继承自原始的类，因此两个类并不存在继承关系，所以想要mock替换原始的类，必须在编译时去做（specify your choice at compile time）。

一种方法是在源代码中使用模板：

    template <class PacketStream>
    void CreateConnection(PacketStream* stream) { ... }

    template <class PacketStream>
    class PacketReader {
     public:
      void ReadPackets(PacketStream* stream, size_t packet_num);
    };

就可以在测试中使用`CreateConnection<MockPacketStream>()` 和 `PacketReader<MockPacketStream>`替代源码中的`CreateConnection<ConcretePacketStream>()` 和 `PacketReader<ConcretePacketStream>`。

    MockPacketStream mock_stream;
      EXPECT_CALL(mock_stream, ...)...;
      .. set more expectations on mock_stream ...
      PacketReader<MockPacketStream> reader(&mock_stream);
      ... exercise reader ...


**这里，有可能需要反过来修改源代码，因此，对源代码是有侵入的，这是gmock的一个缺点**。


## 参考文档
googlemock官方文档：
https://github.com/google/googletest/blob/master/googlemock/README.md
https://github.com/google/googletest/blob/master/googlemock/docs/CheatSheet.md
https://github.com/google/googletest/blob/master/googlemock/docs/CookBook.md

https://www.cnblogs.com/welkinwalker/archive/2011/11/29/2267225.html
