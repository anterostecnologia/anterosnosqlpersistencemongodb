/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package br.com.anteros.nosql.persistence.mongodb.session;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.function.BiFunction;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.bson.Document;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import br.com.anteros.core.utils.Assert;
import br.com.anteros.core.utils.ClassUtils;
import br.com.anteros.core.utils.ConcurrentReferenceHashMap;
import br.com.anteros.core.utils.ReflectionUtils;

public class SessionAwareMethodInterceptor<D, C> implements MethodInterceptor {

	private static final MethodCache METHOD_CACHE = new MethodCache();

	private final ClientSession session;
	private final ClientSessionOperator collectionDecorator;
	private final ClientSessionOperator databaseDecorator;
	private final Object target;
	private final Class<?> targetType;
	private final Class<?> collectionType;
	private final Class<?> databaseType;
	private final Class<? extends ClientSession> sessionType;

	public <T> SessionAwareMethodInterceptor(ClientSession session, T target, Class<ClientSession> sessionType,
			Class<D> databaseType, ClientSessionOperator<D> databaseDecorator, Class<C> collectionType,
			ClientSessionOperator<C> collectionDecorator) {

		Assert.notNull(session, "ClientSession must not be null!");
		Assert.notNull(target, "Target must not be null!");
		Assert.notNull(sessionType, "SessionType must not be null!");
		Assert.notNull(databaseType, "Database type must not be null!");
		Assert.notNull(databaseDecorator, "Database ClientSessionOperator must not be null!");
		Assert.notNull(collectionType, "Collection type must not be null!");
		Assert.notNull(collectionDecorator, "Collection ClientSessionOperator must not be null!");

		this.session = session;
		this.target = target;
		this.databaseType = ClassUtils.getUserClass(databaseType);
		this.collectionType = ClassUtils.getUserClass(collectionType);
		this.collectionDecorator = collectionDecorator;
		this.databaseDecorator = databaseDecorator;

		this.targetType = ClassUtils.isAssignable(databaseType, target.getClass()) ? databaseType : collectionType;
		this.sessionType = sessionType;
	}

	@Override
	public Object invoke(MethodInvocation methodInvocation) throws Throwable {

		if (requiresDecoration(methodInvocation.getMethod())) {

			Object target = methodInvocation.proceed();
			if (target instanceof Proxy) {
				return target;
			}

			return decorate(target);
		}

		if (!requiresSession(methodInvocation.getMethod())) {
			return methodInvocation.proceed();
		}

		Optional<Method> targetMethod = METHOD_CACHE.lookup(methodInvocation.getMethod(), targetType, sessionType);

		return !targetMethod.isPresent() ? methodInvocation.proceed()
				: ReflectionUtils.invokeMethod(targetMethod.get(), target,
						prependSessionToArguments(session, methodInvocation));
	}

	private boolean requiresDecoration(Method method) {

		return ClassUtils.isAssignable(databaseType, method.getReturnType())
				|| ClassUtils.isAssignable(collectionType, method.getReturnType());
	}

	@SuppressWarnings("unchecked")
	protected Object decorate(Object target) {

		return ClassUtils.isAssignable(databaseType, target.getClass()) ? databaseDecorator.apply(session, target)
				: collectionDecorator.apply(session, target);
	}

	private static boolean requiresSession(Method method) {

		if (method.getParameterCount() == 0
				|| !ClassUtils.isAssignable(ClientSession.class, method.getParameterTypes()[0])) {
			return true;
		}

		return false;
	}

	private static Object[] prependSessionToArguments(ClientSession session, MethodInvocation invocation) {

		Object[] args = new Object[invocation.getArguments().length + 1];

		args[0] = session;
		System.arraycopy(invocation.getArguments(), 0, args, 1, invocation.getArguments().length);

		return args;
	}

	static class MethodCache {

		private final ConcurrentReferenceHashMap<MethodClassKey, Optional<Method>> cache = new ConcurrentReferenceHashMap<>();

		Optional<Method> lookup(Method method, Class<?> targetClass, Class<? extends ClientSession> sessionType) {

			return cache.computeIfAbsent(new MethodClassKey(method, targetClass),
					val -> Optional.ofNullable(findTargetWithSession(method, targetClass, sessionType)));
		}

		private Method findTargetWithSession(Method sourceMethod, Class<?> targetType,
				Class<? extends ClientSession> sessionType) {

			Class<?>[] argTypes = sourceMethod.getParameterTypes();
			Class<?>[] args = new Class<?>[argTypes.length + 1];
			args[0] = sessionType;
			System.arraycopy(argTypes, 0, args, 1, argTypes.length);

			return ReflectionUtils.findMethod(targetType, sourceMethod.getName(), args);
		}

		boolean contains(Method method, Class<?> targetClass) {
			return cache.containsKey(new MethodClassKey(method, targetClass));
		}
	}

	public interface ClientSessionOperator<T> extends BiFunction<ClientSession, T, T> {
	}
}
