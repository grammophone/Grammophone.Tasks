using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Grammophone.Tasks
{
	/// <summary>
	/// A container for task queues assigned to channels.
	/// </summary>
	/// <typeparam name="C">The type of the channels.</typeparam>
	public class ChannelsTaskQueuer<C>
	{
		#region Private fields

		private readonly ConcurrentDictionary<C, Task> tasksByChannel;

		private readonly TaskCreationOptions taskCreationOptions;

		private readonly TaskContinuationOptions taskContinuationOptions;

		#endregion

		#region Construction

		/// <summary>
		/// Create.
		/// </summary>
		/// <param name="attachTasksToParent">If true, the tasks are attached to the parent task where the scheduler lives, else they are independent.</param>
		public ChannelsTaskQueuer(bool attachTasksToParent)
		{
			if (attachTasksToParent)
			{
				taskCreationOptions = TaskCreationOptions.AttachedToParent;
				taskContinuationOptions = TaskContinuationOptions.AttachedToParent;
			}
			else
			{
				taskCreationOptions = TaskCreationOptions.None;
				taskContinuationOptions = TaskContinuationOptions.None;
			}

			tasksByChannel = new ConcurrentDictionary<C, Task>();
		}

		/// <summary>
		/// Create.
		/// </summary>
		/// <param name="taskCreationOptions">The task creation options used for the root task of a channel.</param>
		/// <param name="taskContinuationOptions">The task continuation options used for a subsequent task of a channel.</param>
		public ChannelsTaskQueuer(TaskCreationOptions taskCreationOptions, TaskContinuationOptions taskContinuationOptions)
		{
			this.taskCreationOptions = taskCreationOptions;
			this.taskContinuationOptions = taskContinuationOptions;

			this.tasksByChannel = new ConcurrentDictionary<C, Task>();
		}

		/// <summary>
		/// Create with default task creation and continuation options.
		/// </summary>
		public ChannelsTaskQueuer()
			: this(false)
		{
		}

		#endregion

		#region Public methods

		/// <summary>
		/// Queue an action for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="action">The action to be executed.</param>
		/// <returns>Returns the task created and queued.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="action"/> is null.</exception>
		public Task QueueAction(C channel, Action action)
			=> QueueAction(channel, action, CancellationToken.None);

		/// <summary>
		/// Queue an action for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="action">The action to be executed.</param>
		/// <param name="cancellationToken">The cancellation token to be assigned to the new task.</param>
		/// <returns>Returns the task created and queued.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="action"/> is null.</exception>
		public Task QueueAction(C channel, Action action, CancellationToken cancellationToken)
		{
			if (action == null) throw new ArgumentNullException(nameof(action));

			var newTask = tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(action, cancellationToken, taskCreationOptions, TaskScheduler.Default),
				(_, existingTask) => existingTask.ContinueWith(pt => action(), cancellationToken, taskContinuationOptions, TaskScheduler.Default));

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Queue an asynchronous action for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="asyncAction">The asynchronous action to be executed.</param>
		/// <returns>Returns the task created and queued.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="asyncAction"/> is null.</exception>
		public Task QueueAsyncAction(C channel, Func<Task> asyncAction)
			=> QueueAsyncAction(channel, asyncAction, CancellationToken.None);

		/// <summary>
		/// Queue an asynchronous action for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="asyncAction">The asynchronous action to be executed.</param>
		/// <param name="cancellationToken">The cancellation token to be assigned to the new task.</param>
		/// <returns>Returns the task created and queued.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="asyncAction"/> is null.</exception>
		public Task QueueAsyncAction(C channel, Func<Task> asyncAction, CancellationToken cancellationToken)
		{
			if (asyncAction == null) throw new ArgumentNullException(nameof(asyncAction));

			var newTask = tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(asyncAction, cancellationToken, taskCreationOptions, TaskScheduler.Default),
				(_, existingTask) => existingTask.ContinueWith(pt => asyncAction(), cancellationToken, taskContinuationOptions, TaskScheduler.Default));

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Queue a function returning a value for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling or to obtain the returned value.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="function">The function to be executed in the task.</param>
		/// <returns>Returns the task created and queued, whose <see cref="Task{TResult}.Result"/> will hold the result of the function.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="function"/> is null.</exception>
		public Task<R> QueueFunction<R>(C channel, Func<R> function)
			=> QueueFunction(channel, function, CancellationToken.None);

		/// <summary>
		/// Queue a function returning a value for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling or to obtain the returned value.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="function">The function to be executed in the task.</param>
		/// <param name="cancellationToken">The cancellation token to be assigned to the new task.</param>
		/// <returns>Returns the task created and queued, whose <see cref="Task{TResult}.Result"/> will hold the result of the function.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="function"/> is null.</exception>
		public Task<R> QueueFunction<R>(C channel, Func<R> function, CancellationToken cancellationToken)
		{
			if (function == null) throw new ArgumentNullException(nameof(function));

			var newTask = (Task<R>)tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(function, cancellationToken, taskCreationOptions, TaskScheduler.Default),
				(_, existingtask) => existingtask.ContinueWith(pt => function(), cancellationToken, taskContinuationOptions, TaskScheduler.Default));

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Queue an asynchronous function returning a value for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling or to obtain the returned value.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="asyncFunction">The function to be executed in the task.</param>
		/// <returns>Returns the task created and queued, whose <see cref="Task{TResult}.Result"/> will hold the result of the function.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="asyncFunction"/> is null.</exception>
		public Task<R> QueueAsyncFunction<R>(C channel, Func<Task<R>> asyncFunction)
			=> QueueAsyncFunction(channel, asyncFunction, CancellationToken.None);

		/// <summary>
		/// Queue an asynchronous function returning a value for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling or to obtain the returned value.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="asyncFunction">The function to be executed in the task.</param>
		/// <param name="cancellationToken">The cancellation token to be assigned to the new task.</param>
		/// <returns>Returns the task created and queued, whose <see cref="Task{TResult}.Result"/> will hold the result of the function.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="asyncFunction"/> is null.</exception>
		public Task<R> QueueAsyncFunction<R>(C channel, Func<Task<R>> asyncFunction, CancellationToken cancellationToken)
		{
			if (asyncFunction == null) throw new ArgumentNullException(nameof(asyncFunction));

			var newTask = (Task<R>)tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(async () => await asyncFunction(), cancellationToken, taskCreationOptions, TaskScheduler.Default).Unwrap(),
				(_, existingtask) => existingtask.ContinueWith(async pt => await asyncFunction(), cancellationToken, taskContinuationOptions, TaskScheduler.Default).Unwrap());

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Get a task which completes when all the queued tasks for all channels complete.
		/// </summary>
		public Task WhenAll()
		{
			var currentTasks = from entry in tasksByChannel
												 select entry.Value;

			return Task.WhenAll(currentTasks);
		}

		#endregion

		#region Protected methods

		/// <summary>
		/// Called when any of the queued tasks fails with an exception.
		/// Override to attach central logging. The default implemention writes to <see cref="System.Diagnostics.Trace"/>.
		/// </summary>
		/// <param name="channel">The channel where the task is queued.</param>
		/// <param name="exception">The exception occured while the task was executed.</param>
		protected virtual void HandleException(C channel, Exception exception)
		{
			System.Diagnostics.Trace.WriteLine($"Error executing task for channel '{channel.ToString()}': {exception.ToString()} ");
		}

		#endregion
	}
}
