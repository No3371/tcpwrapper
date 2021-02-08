# tcpwrapper
A go implementation wrapping go's tcp package.

It allows easily spin-up a tcp connection both client/server side, it also use github.com/smallnest/epoller to implement epoll to serve massive amount of clients, though the effect is not guaranteed to be better then without epoll, considering that go itself already implement epoll in official pacakges.
