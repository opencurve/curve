# GoogleTest

## 基本用法

### TEST()和Test Case的概念

Meaning | Google Test Term | [ISTQB](http://www.istqb.org/) Term
------- | ---------------- | -----------------------------------
Exercise a particular program path with specific input values and verify the results | TEST() | [Test Case](http://glossary.istqb.org/search/test%20case)
A set of several tests related to one component | Test Case | [Test Suite](http://glossary.istqb.org/search/test%20suite)


### 简单示例

    TEST(testCaseName, testName) {
     ... test body ...
    }

    int main(int argc, char **argv) {
      ::testing::InitGoogleTest(&argc, argv);
      return RUN_ALL_TESTS();
    }

### Test Fixtures 测试固件

    TEST_F(test_case_name, test_name) {   
     ... test body ...
    }

创建测试固件的步骤：
1. 创建测试类，继承::testing::Test，并使用public:或protected:访问级别，因为需要在子类中访问测试固件的成员。
2. 在类内部，定义任何你需要的成员
3. 如果需要，编写默认构造函数，以及SetUp函数
4. 如果需要，编写析构函数，以及TearDown函数
5. 如果需要，编写子函数，以便在各测试用例间共享

例子：

    #include "this/package/foo.h"
    #include "gtest/gtest.h"
    namespace {

    // The fixture for testing class Foo.
    class FooTest : public ::testing::Test {
     protected:
      // You can remove any or all of the following functions if its body
      // is empty.

      FooTest() {
        // You can do set-up work for each test here.
      }

      virtual ~FooTest() {
        // You can do clean-up work that doesn't throw exceptions here.
      }

      // If the constructor and destructor are not enough for setting up
      // and cleaning up each test, you can define the following methods:

      virtual void SetUp() {
        // Code here will be called immediately after the constructor (right
        // before each test).
      }

      virtual void TearDown() {
        // Code here will be called immediately after each test (right
        // before the destructor).
      }

      // Objects declared here can be used by all tests in the test case for Foo.
    };

    // Tests that the Foo::Bar() method does Abc.
    TEST_F(FooTest, MethodBarDoesAbc) {
      const string input_filepath = "this/package/testdata/myinputfile.dat";
      const string output_filepath = "this/package/testdata/myoutputfile.dat";
      Foo f;
      EXPECT_EQ(0, f.Bar(input_filepath, output_filepath));
    }

    // Tests that Foo does Xyz.
    TEST_F(FooTest, DoesXyz) {
      // Exercises the Xyz feature of Foo.
    }

    }  // namespace

    int main(int argc, char **argv) {
      ::testing::InitGoogleTest(&argc, argv);
      return RUN_ALL_TESTS();
    }


## 断言

gtest中，断言的宏可以理解为分为两类，一类是ASSERT系列，一类是EXPECT系列。

1. ASSERT\_\* 系列的断言，当检查点失败时，退出当前函数。 
2. EXPECT\_\* 系列的断言，当检查点失败时，继续往下执行。

例子：

    // int型比较，预期值：3，实际值：Add(1, 2)
    EXPECT_EQ(3, Add(1, 2))
    // 

需要提供额外的message，只需要在ASSERT宏或EXPECT宏最后使用 <<  操作符，可以串联使用，例：

    ASSERT_EQ(x.size(), y.size()) << "Vectors x and y are of unequal length";

### 基本断言

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_TRUE(`_condition_`)`;  | `EXPECT_TRUE(`_condition_`)`;   | _condition_ is true |
| `ASSERT_FALSE(`_condition_`)`; | `EXPECT_FALSE(`_condition_`)`;  | _condition_ is false |

### 数值型数据检查

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
|`ASSERT_EQ(`_val1_`, `_val2_`);`|`EXPECT_EQ(`_val1_`, `_val2_`);`| _val1_ `==` _val2_ |
|`ASSERT_NE(`_val1_`, `_val2_`);`|`EXPECT_NE(`_val1_`, `_val2_`);`| _val1_ `!=` _val2_ |
|`ASSERT_LT(`_val1_`, `_val2_`);`|`EXPECT_LT(`_val1_`, `_val2_`);`| _val1_ `<` _val2_ |
|`ASSERT_LE(`_val1_`, `_val2_`);`|`EXPECT_LE(`_val1_`, `_val2_`);`| _val1_ `<=` _val2_ |
|`ASSERT_GT(`_val1_`, `_val2_`);`|`EXPECT_GT(`_val1_`, `_val2_`);`| _val1_ `>` _val2_ |
|`ASSERT_GE(`_val1_`, `_val2_`);`|`EXPECT_GE(`_val1_`, `_val2_`);`| _val1_ `>=` _val2_ |

### 字符串检查

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_STREQ(`_str1_`, `_str2_`);`    | `EXPECT_STREQ(`_str1_`, `_str2_`);`     | the two C strings have the same content |
| `ASSERT_STRNE(`_str1_`, `_str2_`);`    | `EXPECT_STRNE(`_str1_`, `_str2_`);`     | the two C strings have different content |
| `ASSERT_STRCASEEQ(`_str1_`, `_str2_`);`| `EXPECT_STRCASEEQ(`_str1_`, `_str2_`);` | the two C strings have the same content, ignoring case |
| `ASSERT_STRCASENE(`_str1_`, `_str2_`);`| `EXPECT_STRCASENE(`_str1_`, `_str2_`);` | the two C strings have different content, ignoring case |


### 显示返回成功或失败

显示成功：
    SUCCEED();

| Fatal assertion | Nonfatal assertion |
|:----------------|:-------------------|
| FAIL();         | ADD_FAILURE();     |

    TEST(ExplicitTest, Demo)
    {
        ADD_FAILURE() << "There is a failure."; // None Fatal Asserton，继续往下执行。

        //FAIL(); // Fatal Assertion，不往下执行该用例。

        SUCCEED();
    }

### 异常检查

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_THROW(`_statement_, _exception\_type_`);`  | `EXPECT_THROW(`_statement_, _exception\_type_`);`  | _statement_ throws an exception of the given type  |
| `ASSERT_ANY_THROW(`_statement_`);`                | `EXPECT_ANY_THROW(`_statement_`);`                | _statement_ throws an exception of any type        |
| `ASSERT_NO_THROW(`_statement_`);`                 | `EXPECT_NO_THROW(`_statement_`);`                 | _statement_ doesn't throw any exception            |

例子：

    int Foo(int a, int b)
    {
        if (a == 0 || b == 0)
        {
            throw "don't do that";
        }
        int c = a % b;
        if (c == 0)
            return b;
        return Foo(b, c);
    }

    TEST(FooTest, HandleZeroInput)
    {
        EXPECT_ANY_THROW(Foo(10, 0));
        EXPECT_THROW(Foo(0, 5), char*);
    }


### Predicate Assertions

在使用EXPECT_TRUE或ASSERT_TRUE时，有时希望能够输出更加详细的信息，比如检查一个函数的返回值TRUE还是FALSE时，希望能够输出传入的参数是什么，以便失败后好跟踪。因此提供了如下的断言：

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_PRED1(`_pred1, val1_`);`       | `EXPECT_PRED1(`_pred1, val1_`);` | _pred1(val1)_ returns true |
| `ASSERT_PRED2(`_pred2, val1, val2_`);` | `EXPECT_PRED2(`_pred2, val1, val2_`);` |  _pred2(val1, val2)_ returns true |
|  ...                | ...                    | ...          |

例子：

    bool MutuallyPrime(int m, int n)
    {
        return Foo(m , n) > 1;
    }

    TEST(PredicateAssertionTest, Demo)
    {
        int m = 5, n = 6;
        EXPECT_PRED2(MutuallyPrime, m, n);
    }

当失败时，返回错误信息：

error: MutuallyPrime(m, n) evaluates to false, where
m evaluates to 5
n evaluates to 6

如果对这样的输出不满意的话，还可以自定义输出格式，通过如下：


| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_PRED_FORMAT1(`_pred\_format1, val1_`);`        | `EXPECT_PRED_FORMAT1(`_pred\_format1, val1_`);` | _pred\_format1(val1)_ is successful |
| `ASSERT_PRED_FORMAT2(`_pred\_format2, val1, val2_`);` | `EXPECT_PRED_FORMAT2(`_pred\_format2, val1, val2_`);` | _pred\_format2(val1, val2)_ is successful |
| `...`               | `...`                  | `...`        |

例子：

    testing::AssertionResult AssertFoo(const char* m_expr, const char* n_expr, const char* k_expr, int m, int n, int k) {
        if (Foo(m, n) == k)
            return testing::AssertionSuccess();
        testing::Message msg;
        msg << m_expr << " 和 " << n_expr << " 的最大公约数应该是：" << Foo(m, n) << " 而不是：" << k_expr;
        return testing::AssertionFailure(msg);
    }

    TEST(AssertFooTest, HandleFail)
    {
        EXPECT_PRED_FORMAT3(AssertFoo, 3, 6, 2);
    }

### 浮点型检查

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_FLOAT_EQ(`_val1, val2_`);`  | `EXPECT_FLOAT_EQ(`_val1, val2_`);` | the two `float` values are almost equal |
| `ASSERT_DOUBLE_EQ(`_val1, val2_`);` | `EXPECT_DOUBLE_EQ(`_val1, val2_`);` | the two `double` values are almost equal |

这里的almost equal的意思是，两个值之间的误差在4 ULP 之间。

对相近的两个数比较：

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_NEAR(`_val1, val2, abs\_error_`);` | `EXPECT_NEAR`_(val1, val2, abs\_error_`);` | the difference between _val1_ and _val2_ doesn't exceed the given absolute error |

### 类型检查

    ::testing::StaticAssertTypeEq<T1, T2>();

类型检查失败时代码直接编译不通过
例子：

    template <typename T> class FooType {
    public:
        void Bar() { testing::StaticAssertTypeEq<int, T>(); }
    };

    TEST(TypeAssertionTest, Demo)
    {
        FooType<bool> fooType;
        fooType.Bar();
    }

## 在多个测试用例直接共享测试资源

有时候我们需要在多个测试用例之间共享一些测试资源和操作，gtest提供一些机制，在测试用例之前或之后做一些操作，这样的类型有三种：

1. 全局的，所有用例执行前后。
2. TestSuite级别的，在某一批用例中第一个用例前，最后一个用例执行后。
3. TestCase级别的，每个TestCase前后。

### 全局Set-Up 和 Tear-Down

通过写一个类，继承testing::Environment类，实现里面的SetUp和TearDown方法。

1. SetUp()方法在所有用例执行前执行
2. TearDown()方法在所有用例执行后执行

```
    class FooEnvironment : public testing::Environment
    {
    public:
        virtual void SetUp()
        {
            std::cout << "Foo FooEnvironment SetUP" << std::endl;
        }
        virtual void TearDown()
        {
            std::cout << "Foo FooEnvironment TearDown" << std::endl;
        }
    };
```

当然，这样还不够，我们还需要告诉gtest添加这个全局事件，我们需要在main函数中通过testing::AddGlobalTestEnvironment方法将事件挂进来，也就是说，我们可以写很多个这样的类，然后将他们的事件都挂上去。

```
    int main(int argc, _TCHAR* argv[])
    {
        testing::AddGlobalTestEnvironment(new FooEnvironment);
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
    }

```
### TestSuite级别的Set-Up 和 Tear-Down

我们需要写一个类，继承testing::Test，然后实现两个静态方法

1. SetUpTestCase() 方法在第一个TestCase之前执行
2. TearDownTestCase() 方法在最后一个TestCase之后执行


```
    class FooTest : public testing::Test {
    protected:
      static void SetUpTestCase() {
        shared_resource_ = new ... ;
      }
      static void TearDownTestCase() {
        delete shared_resource_;
        shared_resource_ = NULL;
      }
      // Some expensive resource shared by all tests.
      static T* shared_resource_;
    };

```
在编写测试用例时，我们需要使用TEST_F这个宏，第一个参数必须是我们上面类的名字，代表一个TestSuite。

    TEST_F(FooTest, Test1)
    {
        // you can refer to shared_resource here 
    }
    TEST_F(FooTest, Test2)
    {
        // you can refer to shared_resource here 
    }

### TestCase级别的Set-Up 和 Tear-Down

TestCase级别的Set-Up 和 Tear-Down是挂在每个用例执行前后的，实现方式和上面的几乎一样，不过需要实现的是SetUp方法和TearDown方法：

1. SetUp()方法在每个TestCase之前执行
2. TearDown()方法在每个TestCase之后执行

```
    class FooCalcTest:public testing::Test
    {
    protected:
        virtual void SetUp()
        {
            m_foo.Init();
        }
        virtual void TearDown()
        {
            m_foo.Finalize();
        }

        FooCalc m_foo;
    };

    TEST_F(FooCalcTest, HandleNoneZeroInput)
    {
        EXPECT_EQ(4, m_foo.Calc(12, 16));
    }

    TEST_F(FooCalcTest, HandleNoneZeroInput_Error)
    {
        EXPECT_EQ(5, m_foo.Calc(12, 16));
    }
```

## 参数化测试

参数化测试允许你使用不同的参数测试测试用例而不需要写多遍类似的测试用例

首先你需要写一个类，继承自::testing::Test和:testing::WithParamInterface<T> （这是一个接口），或者继承自::testing::TestWithParam<T> （它继承自::testing::Test和:testing::WithParamInterface<T>），
其中T可以是任何可拷贝类型，
例子：

    class FooTest : public ::testing::TestWithParam<const char*> {
      // You can implement all the usual fixture class members here.
      // To access the test parameter, call GetParam() from class
      // TestWithParam<T>.
    };

    // Or, when you want to add parameters to a pre-existing fixture class:
    class BaseTest : public ::testing::Test {
      ...
    };
    class BarTest : public BaseTest,
                    public ::testing::WithParamInterface<const char*> {
      ...
    };


然后，使用TEST_P宏，编写测试固件，如下：

    TEST_P(FooTest, DoesBlah) {
      // Inside a test, access the test parameter with the GetParam() method
      // of the TestWithParam<T> class:
      EXPECT_TRUE(foo.Blah(GetParam()));
      ...
    }

    TEST_P(FooTest, HasBlahBlah) {
      ...
    }

最后，使用INSTANTIATE_TEST_CASE_P 宏，使用参数实例化测试用例。

    INSTANTIATE_TEST_CASE_P(InstantiationName,
                            FooTest,
                            ::testing::Values("meeny", "miny", "moe"));


第一个参数是测试用例的前缀，可以任意取。 
第二个参数是测试用例的名称，需要和之前定义的参数化的类的名称相同，如：FooTest 
第三个参数是可以理解为参数生成器，上面的例子使用test::Values表示使用括号内的参数。Google提供了一系列的参数生成的函数：

| `Range(begin, end[, step])` | 范围在begin~end之间，步长为step，不包括end |
|:----------------------------|:----------------------------|
| `Values(v1, v2, ..., vN)`   | 取值范围`{v1, v2, ..., vN}`. |
| `ValuesIn(container)` and `ValuesIn(begin, end)` | 从一个C类型的数组或是STL容器，或是迭代器中取值 |
| `Bool()`                    | 取值范围`{false, true}`. |
| `Combine(g1, g2, ..., gN)`  | 将g1,g2,...gN进行排列组合，g1,g2,...gN本身是一个参数生成器，每次分别从g1,g2,..gN中各取出一个值，组合成一个元组(Tuple)作为一个参数。这个功能只在提供了`<tr1/tuple>`头文件的系统中有效。gtest会自动去判断是否支持tr/tuple，如果你的系统确实支持，而gtest判断错误的话，你可以重新定义宏GTEST_HAS_TR1_TUPLE=1。| 

## 类型参数化测试

### 类型测试

gtest还提供了应付各种不同类型的数据时的方案，以及参数化类型的方案。

首先定义一个模版类，继承testing::Test：

    template <typename T>
    class FooTest : public testing::Test {
     public:
      
      typedef std::list<T> List;
      static T shared_;
      T value_;
    };

接着我们定义需要测试到的具体数据类型，比如下面定义了需要测试char,int和unsigned int ：

    typedef testing::Types<char, int, unsigned int> MyTypes;
    TYPED_TEST_CASE(FooTest, MyTypes);

最后，使用TYPED_TEST()代替TEST_F()，定义测试用例：

    TYPED_TEST(FooTest, DoesBlah) {
      // Inside a test, refer to the special name TypeParam to get the type
      // parameter.  Since we are inside a derived class template, C++ requires
      // us to visit the members of FooTest via 'this'.
      TypeParam n = this->value_;

      // To visit static members of the fixture, add the 'TestFixture::'
      // prefix.
      n += TestFixture::shared_;

      // To refer to typedefs in the fixture, add the 'typename TestFixture::'
      // prefix.  The 'typename' is required to satisfy the compiler.
      typename TestFixture::List values;
      values.push_back(n);
      
    }


### 类型参数化测试

类型参数化测试类似于类型测试，但是它**不需要事先知道测试类型**。

首先定义一个模版类：

    template <typename T>
    class FooTest : public ::testing::Test {
      ...
    };

接下来，定义参数化类型测试集，使用宏TYPED_TEST_CASE_P：

    TYPED_TEST_CASE_P(FooTest);

之后，使用宏TYPED_TEST_P来编写测试用例:

    TYPED_TEST_P(FooTest, DoesBlah) {
      // Inside a test, refer to TypeParam to get the type parameter.
      TypeParam n = 0;
      ...
    }

    TYPED_TEST_P(FooTest, HasPropertyA) { ... }

接着，我们需要使用REGISTER_TYPED_TEST_CASE_P宏来注册测试用例，第一个参数是testcase的名称，后面的参数是test的名称

    REGISTER_TYPED_TEST_CASE_P(FooTest,
                               DoesBlah, HasPropertyA);

最后，指定需要测试的类型列表：

    typedef ::testing::Types<char, int, unsigned int> MyTypes;
    INSTANTIATE_TYPED_TEST_CASE_P(My, FooTest, MyTypes);


## 死亡测试

通常在测试过程中，我们需要考虑各种各样的输入，有的输入可能直接导致程序崩溃，这时我们就需要检查程序是否按照预期的方式挂掉，这也就是所谓的“死亡测试”。gtest的死亡测试能做到在一个安全的环境下执行崩溃的测试案例，同时又对崩溃结果进行验证。

### 使用的宏

googletest 提供如下宏来支持死亡测试

| **Fatal assertion** | **Nonfatal assertion** | **Verifies** |
|:--------------------|:-----------------------|:-------------|
| `ASSERT_DEATH(`_statement, regex_`);` | `EXPECT_DEATH(`_statement, regex_`);` | _statement_ crashes with the given error |
| `ASSERT_DEATH_IF_SUPPORTED(`_statement, regex_`);` | `EXPECT_DEATH_IF_SUPPORTED(`_statement, regex_`);` | if death tests are supported, verifies that _statement_ crashes with the given error; otherwise verifies nothing |
| `ASSERT_EXIT(`_statement, predicate, regex_`);` | `EXPECT_EXIT(`_statement, predicate, regex_`);` |_statement_ exits with the given error and its exit code matches _predicate_ |

 
### *_DEATH(statement, regex)

1. statement是被测试的代码语句
2. regex是一个正则表达式，用来匹配异常时在stderr中输出的内容


    void Foo()
    {
        int *pInt = 0;
        *pInt = 42 ;
    }

    TEST(FooDeathTest, Demo)
    {
        EXPECT_DEATH(Foo(), "");
    }


### *_EXIT(statement, predicate, regex)

1. statement是被测试的代码语句
2. predicate 在这里必须是一个委托，接收int型参数，并返回bool。只有当返回值为true时，死亡测试案例才算通过。gtest提供了一些常用的predicate：


    testing::ExitedWithCode(exit_code)

如果程序正常退出并且退出码与exit_code相同则返回 true


    testing::KilledBySignal(signal_number)

如果程序被signal_number信号kill的话就返回true

3. regex是一个正则表达式，用来匹配异常时在stderr中输出的内容

例子：

    TEST(ExitDeathTest, Demo)
    {
        EXPECT_EXIT(_exit(1),  testing::ExitedWithCode(1),  "");
    }


## 测试私有代码

如果我们改变了软件的内部实现的时候，并没有改变代码的外部表现，原则上测试用例不应该失效，应当遵循黑盒测试的原则，大多数情况下只测试公有接口。

如果仍然需要测试内部实现代码，那么应当回头考虑代码实现是否合适。

如果在此之后，仍然需要测试内部实现代码，那么有两种情况：

1. 静态函数（不是静态成员函数），或匿名命名空间内的函数
2. 类的私有或保护成员


### 静态函数

静态函数和匿名命名空间内的函数具有文件作用域，也就是仅在当前文件内可见。 
可将实现静态函数的源文件使用#include包含到测试代码中来进行测试。

### 私有类成员

1. 使用右元来实现测试类的私有成员

将测试固件类申明为类的友元，来使得测试固件类可以访问类的私有成员。这里要注意的是，虽然测试固件类是被测类的友元，但测试用例仍然不是被测类的友元，因为通常测试用例的类是测试固件类的子类。

2. 使用FRIEND_TEST(TestCaseName, TestName)声明，将独立的测试用例声明在类内

例子：

    // foo.h
    #include "gtest/gtest_prod.h"

    // Defines FRIEND_TEST.
    class Foo {
      ...
     private:
      FRIEND_TEST(FooTest, BarReturnsZeroOnNull);
      int Bar(void* x);
    };

    // foo_test.cc
    ...
    TEST(FooTest, BarReturnsZeroOnNull) {
      Foo foo;
      EXPECT_EQ(0, foo.Bar(NULL));
      // Uses Foo's private member Bar().
    }
