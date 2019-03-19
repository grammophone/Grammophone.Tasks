# Grammophone.Tasks
This .NET Standard 2.0 library contains utlilties built on Task Parallel Library. The library has no dependencies.

### `ChannelsTaskQueuer<C>`
This class maintains separate task queues per individual channels and appends tasks to them.
A channel can be of any type `C`.

![ChannelsTaskQueuer UML diagram](https://github.com/grammophone/Grammophone.Tasks/blob/master/Images/ChannelsTaskQueuer.png)

Use one of the `QueueTask` methods to enqueue a task.
Exception handling can be central by overriding `HandleException` method.
Exception handling can be done on a per-task basis as well, by continuing the task
returned by `QueueTask` methods with `TaskContinuationOptions.OnlyOnFaulted` option as usual.

The default implementation of `HandleException` method writes to `System.Diagnostics.Trace`.
