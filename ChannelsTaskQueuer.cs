using System;
using System.Collections.Concurrent;
using System.Linq;
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
		/// <param name="attachTasksToParent">If true, the tasks are attached to the parent task where the scheduler lives, else tey are independent.</param>
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
		/// Queue a task for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="action">The action to be executed in the task.</param>
		/// <returns>Returns the task created and queued.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="action"/> is null.</exception>
		public Task QueueTask(C channel, Action action)
		{
			if (action == null) throw new ArgumentNullException(nameof(action));

			var newTask = tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(action, taskCreationOptions),
				(_, existingTask) => existingTask.ContinueWith(pt => action(), taskContinuationOptions));

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Queue a task for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="action">The action to be executed in the task.</param>
		/// <param name="cancellationToken">The cancellation token to be assigned to the new task.</param>
		/// <returns>Returns the task created and queued.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="action"/> is null.</exception>
		public Task QueueTask(C channel, Action action, System.Threading.CancellationToken cancellationToken)
		{
			if (action == null) throw new ArgumentNullException(nameof(action));

			var newTask = tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(action, cancellationToken, taskCreationOptions, Task.Factory.Scheduler),
				(_, existingTask) => existingTask.ContinueWith(pt => action(), cancellationToken, taskContinuationOptions, Task.Factory.Scheduler));

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Queue a task returning a value for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling or to obtain the returned value.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="function">The function to be executed in the task.</param>
		/// <returns>Returns the task created and queued, whose <see cref="Task{TResult}.Result"/> will hold the result of the function.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="function"/> is null.</exception>
		public Task<R> QueueTask<R>(C channel, Func<R> function)
		{
			if (function == null) throw new ArgumentNullException(nameof(function));

			var newTask = (Task<R>)tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(function, taskCreationOptions),
				(_, existingTask) => existingTask.ContinueWith(pt => function(), taskContinuationOptions));

			newTask.ContinueWith(t => HandleException(channel, t.Exception), TaskContinuationOptions.OnlyOnFaulted);

			return newTask;
		}

		/// <summary>
		/// Queue a task returning a value for a channel. Exceptions are handled in <see cref="HandleException(C, Exception)"/>.
		/// Use the returned task to attach additional exception handling or to obtain the returned value.
		/// </summary>
		/// <param name="channel">The channel for which to queue the task.</param>
		/// <param name="function">The function to be executed in the task.</param>
		/// <param name="cancellationToken">The cancellation token to be assigned to the new task.</param>
		/// <returns>Returns the task created and queued, whose <see cref="Task{TResult}.Result"/> will hold the result of the function.</returns>
		/// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> or <paramref name="function"/> is null.</exception>
		public Task<R> QueueTask<R>(C channel, Func<R> function, System.Threading.CancellationToken cancellationToken)
		{
			if (function == null) throw new ArgumentNullException(nameof(function));

			var newTask = (Task<R>)tasksByChannel.AddOrUpdate(
				channel,
				_ => Task.Factory.StartNew(function, cancellationToken, taskCreationOptions, Task.Factory.Scheduler),
				(_, existingtask) => existingtask.ContinueWith(pt => function(), cancellationToken, taskContinuationOptions, Task.Factory.Scheduler));

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
