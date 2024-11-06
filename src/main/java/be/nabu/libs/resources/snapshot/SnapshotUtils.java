/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.resources.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import be.nabu.libs.resources.ResourceUtils;
import be.nabu.libs.resources.api.DetachableResource;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.resources.api.features.CacheableResource;
import be.nabu.utils.aspects.AspectUtils;
import be.nabu.utils.aspects.api.NotImplemented;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;

public class SnapshotUtils {
	
	public static class PreparedResource implements Resource {
		private Resource resource;
		private ResourceContainer<?> parent;
		
		public PreparedResource(Resource resource, ResourceContainer<?> parent) {
			this.resource = resource;
			this.parent = parent;
		}
		@Override
		@NotImplemented
		public String getContentType() {
			return null;
		}
		@Override
		@NotImplemented
		public String getName() {
			return null;
		}
		@Override
		public ResourceContainer<?> getParent() {
//			if (parent == null) {
//				if (resource.getParent() != null) {
//					if (AspectUtils.hasAspects(resource.getParent())) {
//						this.parent = resource.getParent();
//					}
//					else {
//						this.parent = prepare(resource.getParent());
//					}
//				}
//			}
			return parent;
		}
		protected Resource getResource() {
			return resource;
		}
	}
	
	public static class PreparedResourceContainer<T extends Resource> extends PreparedResource implements ResourceContainer<T>, CacheableResource {
		
		Map<String, Resource> preparedResources;
		
		private boolean caching = true;

		private ResourceContainer<?> dynamic;
		
		public PreparedResourceContainer(ResourceContainer<?> resource, ResourceContainer<?> parent, ResourceContainer<?> dynamic) {
			super(resource, parent);
			this.dynamic = dynamic;
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public Iterator iterator() {
			return getChildren().values().iterator();
		}
		Map<String, Resource> getChildren() {
			if (preparedResources == null) {
				synchronized(this) {
					if (preparedResources == null) {
						Map<String, Resource> preparedResources = new HashMap<String, Resource>();
						for (Resource child : (ResourceContainer<?>) getResource()) {
							preparedResources.put(child.getName(), prepare(child, dynamic));
						}
						this.preparedResources = preparedResources;
					}
				}
			}
			return preparedResources;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T getChild(String name) {
			return (T) getChildren().get(name);
		}
		@Override
		public void resetCache() throws IOException {
			if (getResource() instanceof CacheableResource) {
				((CacheableResource) getResource()).resetCache();
			}
			preparedResources = null;
		}
		@Override
		public void setCaching(boolean cache) {
			this.caching = cache;
		}
		@Override
		public boolean isCaching() {
			return caching;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static class PreparedManageableContainer<T extends Resource> extends PreparedResourceContainer<T> implements ManageableContainer<T> {

		public PreparedManageableContainer(ManageableContainer<?> container, ResourceContainer<?> parent, ResourceContainer<?> dynamic) {
			super(container, parent, dynamic);
		}
		
		@Override
		public T create(String name, String contentType) throws IOException {
			Resource create = ((ManageableContainer<?>) getResource()).create(name, contentType);
			if (create != null) {
				create = prepare(create, this);
				synchronized(this) {
					getChildren().put(name, create);
				}
			}
			return (T) create;
		}

		@Override
		public void delete(String name) throws IOException {
			((ManageableContainer<?>) getResource()).delete(name);
			synchronized(this) {
				getChildren().remove(name);
			}
		}
		
	}

	@SuppressWarnings("rawtypes")
	private static final class ResourceContainerSnapshot implements ResourceContainer {
		private final List<Resource> children;

		private ResourceContainerSnapshot(List<Resource> children) {
			this.children = children;
		}
		@Override
		@NotImplemented
		public String getContentType() {
			return null;
		}
		@Override
		@NotImplemented
		public String getName() {
			return null;
		}
		@NotImplemented
		@Override
		public ResourceContainer<?> getParent() {
			return null;
		}
		@Override
		public Iterator iterator() {
			return children.iterator();
		}
		@Override
		public Resource getChild(String name) {
			for (Resource child : children) {
				if (child.getName().equals(name)) {
					return child;
				}
			}
			return null;
		}
	}
	
	@SuppressWarnings("rawtypes")
	private static final class ManageableContainerSnapshot implements ManageableContainer {
		@Override
		@NotImplemented
		public String getContentType() {
			return null;
		}
		@Override
		@NotImplemented
		public String getName() {
			return null;
		}
		@NotImplemented
		@Override
		public ResourceContainer<?> getParent() {
			return null;
		}
		@NotImplemented
		@Override
		public Iterator iterator() {
			return null;
		}
		@NotImplemented
		@Override
		public Resource getChild(String name) {
			return null;
		}
		@Override
		public Resource create(String name, String contentType) throws IOException {
			throw new IllegalStateException("Can not create a resource when the container is snapshotted");
		}
		@Override
		public void delete(String name) throws IOException {
			throw new IllegalStateException("Can not delete a resource when the container is snapshotted");
		}
	}

	private static final class ReadableResourceSnapshot implements ReadableResource {
		private final byte[] content;
		private ReadableResourceSnapshot(byte[] content) {
			this.content = content;
		}
		@Override
		@NotImplemented
		public String getContentType() {
			return null;
		}
		@Override
		@NotImplemented
		public String getName() {
			return null;
		}
		@NotImplemented
		@Override
		public ResourceContainer<?> getParent() {
			return null;
		}
		@Override
		public ReadableContainer<ByteBuffer> getReadable() throws IOException {
			return IOUtils.wrap(content, true);
		}
	}
	
	private static final class CacheableResourceSnapshot implements CacheableResource {
		@Override
		@NotImplemented
		public String getContentType() {
			return null;
		}
		@Override
		@NotImplemented
		public String getName() {
			return null;
		}
		@NotImplemented
		@Override
		public ResourceContainer<?> getParent() {
			return null;
		}
		@Override
		public void resetCache() throws IOException {
			// do not reset the cache while it is snapshotted
		}
		@Override
		public void setCaching(boolean cache) {
			// sure buddy, i'll "disable" it *wink* *wink*
		}
		@Override
		public boolean isCaching() {
			return true;
		}
		
	}
	
	private static final class WritableResourceSnapshot implements WritableResource {
		@Override
		@NotImplemented
		public String getContentType() {
			return null;
		}
		@Override
		@NotImplemented
		public String getName() {
			return null;
		}
		@NotImplemented
		@Override
		public ResourceContainer<?> getParent() {
			return null;
		}
		@Override
		public WritableContainer<ByteBuffer> getWritable() {
			throw new IllegalStateException("Can not write to a resource when it is snapshotted");
		}
	}

	public static <T extends Resource> T prepare(final T resource) {
		return prepare(resource, null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static <T extends Resource> T prepare(final T resource, ResourceContainer<?> parent) {
		// we make it dynamic
		T dynamic = AspectUtils.join(resource);
		if (resource instanceof ManageableContainer) {
			AspectUtils.add(dynamic, new PreparedManageableContainer((ManageableContainer) resource, parent, (ResourceContainer) dynamic));
		}
		else if (resource instanceof ResourceContainer) {
			AspectUtils.add(dynamic, new PreparedResourceContainer((ResourceContainer) resource, parent, (ResourceContainer) dynamic));
		}
		else {
			AspectUtils.add(dynamic, new PreparedResource(resource, parent));
		}
		if (resource instanceof DetachableResource) {
			AspectUtils.add(dynamic, new DetachableResource() {
				@Override
				@NotImplemented
				public String getContentType() {
					return null;
				}
				@Override
				@NotImplemented
				public String getName() {
					return null;
				}
				@Override
				@NotImplemented
				public ResourceContainer<?> getParent() {
					return null;
				}
				@Override
				public Resource detach() {
					return prepare(((DetachableResource) resource).detach());
				}
				
			});
		}
		return dynamic;
	}
	
	public static void snapshot(final Resource resource, boolean recursive) {
		// if the original is a resource container, snapshot the children
		if (resource instanceof ResourceContainer) {
			final List<Resource> children = new ArrayList<Resource>();
			for (Resource child : (ResourceContainer<?>) resource) {
				if (recursive) {
					snapshot(child, recursive);
				}
				children.add(child);
			}
			AspectUtils.add(resource, new ResourceContainerSnapshot(children));
		}
		// if it is readable, snapshot the contents
		if (resource instanceof ReadableResource) {
			try {
				ReadableContainer<ByteBuffer> readable = ((ReadableResource) resource).getReadable();
				try {
					final byte [] content = IOUtils.toBytes(readable);
					AspectUtils.add(resource, new ReadableResourceSnapshot(content));
				}
				finally {
					readable.close();
				}
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		if (resource instanceof WritableResource) {
			AspectUtils.add(resource, new WritableResourceSnapshot());
		}
		if (resource instanceof ManageableContainer) {
			AspectUtils.add(resource, new ManageableContainerSnapshot());
		}
		// make sure we can't reset the cache while it is snapshotted
		if (resource instanceof CacheableResource) {
			AspectUtils.add(resource, new CacheableResourceSnapshot());
		}
	}
	
	public static void release(final Resource resource, boolean recursive) {
		AspectUtils.remove(resource, ResourceContainerSnapshot.class, ReadableResourceSnapshot.class, WritableResourceSnapshot.class, ManageableContainerSnapshot.class, CacheableResourceSnapshot.class);
		if (recursive && resource instanceof ResourceContainer) {
			for (Resource child : (ResourceContainer<?>) resource) {
				release(child, recursive);
			}
		}
		if (resource instanceof CacheableResource) {
			try {
				((CacheableResource) resource).resetCache();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static void restore(final Resource resource, boolean recursive) {
		List<Object> aspects = AspectUtils.aspects(resource);
		// the original resource
		Resource original = (Resource) aspects.get(0);
		try {
			// delete whatever is in its place now (if anything)
			if (original.getParent() != null && original.getParent().getChild(original.getName()) != null) {
				((ManageableContainer<?>) original.getParent()).delete(original.getName());
			}
			// copy the snapshotted contents to the parent
			ResourceUtils.copy(resource, (ManageableContainer<?>) original.getParent());
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static boolean isSnapshotted(final Resource resource) {
		List<Object> aspects = AspectUtils.aspects(resource);
		for (Object aspect : aspects) {
			if (aspect instanceof ReadableResourceSnapshot || aspect instanceof ResourceContainerSnapshot || aspect instanceof WritableResourceSnapshot || aspect instanceof ManageableContainerSnapshot || aspect instanceof CacheableResourceSnapshot) {
				return true;
			}
		}
		return false;
	}
}
