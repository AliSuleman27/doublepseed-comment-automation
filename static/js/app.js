const App = {
  templates: {},
  posts: [],
  products: [],
  selectedProduct: null,
  selectedTemplate: null,

  async init() {
    console.log('[DS] init');

    // Check if products were server-rendered
    const sidebarItems = document.querySelectorAll('.sb-item[data-pid]');
    if (sidebarItems.length > 0) {
      // Products already rendered by server
      this.products = Array.from(sidebarItems).map(el => ({
        id: el.dataset.pid,
        title: el.querySelector('span:last-child').textContent.trim()
      }));
      this.setStatus('ready');
    } else {
      // No products rendered — fetch from API (may need service key)
      await this.loadProducts();
    }

    // Auto-select: last used product, or first available
    const lastPid = localStorage.getItem('ds_product');
    const lastTid = localStorage.getItem('ds_template');
    const validPid = this.products.find(p => p.id === lastPid) ? lastPid :
                     (this.products.length > 0 ? this.products[0].id : null);

    if (validPid) {
      await this.selectProduct(validPid);
      // Restore last template selection if valid
      if (lastTid) {
        const tsel = document.getElementById('templateSelect');
        if (tsel.querySelector(`option[value="${lastTid}"]`)) {
          tsel.value = lastTid;
          this.selectedTemplate = lastTid;
        }
      }
    }
  },

  async loadProducts() {
    this.setStatus('loading');
    try {
      const res = await fetch('/api/products');
      const data = await res.json();

      if (data.error) {
        this.showWarning(data.error);
        this.setStatus('error');
        return;
      }

      if (Array.isArray(data) && data.length > 0) {
        this.products = data;
        this.renderSidebar(data);
        this.renderProductDropdown(data);
        this.setStatus('ready');
      } else {
        // 0 products — likely RLS issue, check diagnostics
        const testRes = await fetch('/api/test');
        const diag = await testRes.json();
        if (!diag.ok) {
          this.showWarning(diag.message);
        } else {
          this.showWarning('No clients found in the database.');
        }
        this.setStatus('error');
      }
    } catch (e) {
      console.error('[DS] Failed to load products:', e);
      this.showWarning('Failed to connect to server: ' + e.message);
      this.setStatus('error');
    }
  },

  renderSidebar(products) {
    const sidebar = document.getElementById('sidebarList');
    const empty = document.getElementById('sidebarEmpty');
    if (empty) empty.remove();

    // Remove existing items
    sidebar.querySelectorAll('.sb-item').forEach(el => el.remove());

    products.forEach(p => {
      const div = document.createElement('div');
      div.className = 'sb-item';
      div.dataset.pid = p.id;
      div.onclick = () => App.selectProduct(p.id);
      div.innerHTML = `<span class="icon">◈</span><span>${this.esc(p.title)}</span>`;
      sidebar.appendChild(div);
    });
  },

  renderProductDropdown(products) {
    const sel = document.getElementById('productSelect');
    sel.innerHTML = '<option value="">Select client...</option>' +
      products.map(p => `<option value="${p.id}">${this.esc(p.title)}</option>`).join('');
  },

  showWarning(msg) {
    const el = document.getElementById('connWarning');
    document.getElementById('connWarningMsg').textContent = msg;
    el.classList.remove('hidden');
  },

  setStatus(state) {
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (state === 'ready') {
      dot.style.background = 'var(--green)';
      txt.textContent = 'Ready';
    } else if (state === 'loading') {
      dot.style.background = 'var(--orange)';
      txt.textContent = 'Loading...';
    } else if (state === 'error') {
      dot.style.background = 'var(--red)';
      txt.textContent = 'Connection Issue';
    }
  },

  async selectProduct(id) {
    if (!id) return;
    this.selectedProduct = id;
    localStorage.setItem('ds_product', id);
    document.querySelectorAll('.sb-item[data-pid]').forEach(el =>
      el.classList.toggle('active', el.dataset.pid === id));
    document.getElementById('productSelect').value = id;
    await this.loadTemplates(id);
  },

  async loadTemplates(productId) {
    const tsel = document.getElementById('templateSelect');
    const fetchBtn = document.getElementById('fetchBtn');

    if (this.templates[productId]) {
      this.renderTemplateDropdown(this.templates[productId]);
      return;
    }

    tsel.innerHTML = '<option value="">Loading templates...</option>';
    tsel.disabled = true;
    fetchBtn.disabled = true;

    try {
      const res = await fetch(`/api/products/${productId}/templates`);
      const data = await res.json();

      if (data.error) {
        tsel.innerHTML = '<option value="">Error: ' + data.error + '</option>';
        return;
      }

      this.templates[productId] = data;
      this.renderTemplateDropdown(data);
    } catch (e) {
      console.error('[DS] template load failed:', e);
      tsel.innerHTML = '<option value="">Error loading templates</option>';
    }
  },

  renderTemplateDropdown(templates) {
    const tsel = document.getElementById('templateSelect');
    const totalPosts = templates.reduce((sum, t) => sum + (t.post_count || 0), 0);
    tsel.innerHTML = '<option value="">All templates (' + templates.length + ') — ' + totalPosts + ' posts</option>' +
      templates.map(t => {
        const label = t.title || 'Template ' + t.id.slice(0, 8);
        const count = t.post_count ? ` [${t.post_count} posts]` : '';
        return `<option value="${t.id}">${label}${count}</option>`;
      }).join('');
    tsel.disabled = false;
    document.getElementById('fetchBtn').disabled = false;
  },

  onProductChange(id) {
    if (id) this.selectProduct(id);
  },

  onTemplateChange() {
    const tid = document.getElementById('templateSelect').value || null;
    this.selectedTemplate = tid;
    if (tid) localStorage.setItem('ds_template', tid);
    else localStorage.removeItem('ds_template');
  },

  async fetchPosts() {
    const pid = this.selectedProduct;
    if (!pid) return;
    const tid = document.getElementById('templateSelect').value || null;
    const hours = parseInt(document.getElementById('hoursSelect').value);

    document.getElementById('fetchBtn').disabled = true;
    document.getElementById('fetchStatus').textContent = 'Fetching...';
    this.setStatus('loading');

    try {
      const res = await fetch('/api/posts/fetch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ product_id: pid, template_id: tid, hours_back: hours }),
      });
      const data = await res.json();
      if (data.error) {
        document.getElementById('fetchStatus').textContent = data.error;
        this.setStatus('error');
        return;
      }

      this.posts = data.posts;
      this.renderStats(data);
      this.renderPosts(data.posts);
      document.getElementById('fetchStatus').textContent = `${data.count} posts found`;
      document.getElementById('exportBtn').classList.toggle('hidden', data.count === 0);
      this.setStatus('ready');
    } catch (e) {
      document.getElementById('fetchStatus').textContent = 'Error: ' + e.message;
      this.setStatus('error');
    } finally {
      document.getElementById('fetchBtn').disabled = false;
    }
  },

  renderStats(data) {
    const row = document.getElementById('statsRow');
    const accounts = new Set(data.posts.map(p => p.account_username));
    const withText = data.posts.filter(p => p.slide_texts && p.slide_texts.length > 0);
    const withLink = data.posts.filter(p => p.tiktok_url);
    row.innerHTML = `
      <div class="stat"><div class="stat-val">${data.count}</div><div class="stat-lbl">Posts</div></div>
      <div class="stat"><div class="stat-val">${accounts.size}</div><div class="stat-lbl">Accounts</div></div>
      <div class="stat"><div class="stat-val">${withText.length}</div><div class="stat-lbl">With Slide Text</div></div>
      <div class="stat"><div class="stat-val">${withLink.length}</div><div class="stat-lbl">With TikTok Link</div></div>
    `;
    row.classList.remove('hidden');
  },

  renderPosts(posts) {
    const card = document.getElementById('postsCard');
    const container = document.getElementById('postsContainer');
    document.getElementById('postsSub').textContent = `${posts.length} succeeded posts`;

    if (!posts.length) {
      container.innerHTML = '<div style="padding:24px; text-align:center; color:var(--text-3);">No posts found for this selection.</div>';
      card.classList.remove('hidden');
      return;
    }

    container.innerHTML = posts.map((p, i) => {
      const texts = (p.slide_texts || []).map((t, j) =>
        `<div class="slide-text"><span class="slide-num">S${j + 1}</span>${this.esc(t)}</div>`
      ).join('');
      const link = p.tiktok_url
        ? `<a href="${p.tiktok_url}" target="_blank" class="post-link">${p.tiktok_url}</a>`
        : '<span style="color:var(--text-3);">No link</span>';
      const time = p.post_time ? new Date(p.post_time).toLocaleString() : '—';
      const hook = p.hook || '';
      const caption = p.caption || p.title || '';

      return `
        <div class="post-row fade-in" style="animation-delay:${Math.min(i * 20, 400)}ms">
          <div class="post-header">
            <div class="post-acct">@${this.esc(p.account_username)}</div>
            <div class="post-meta">${p.num_slides || '?'} slides · ${time}</div>
          </div>
          ${hook ? `<div class="post-hook">${this.esc(hook)}</div>` : ''}
          <div class="post-caption">${this.esc(caption)}</div>
          <div class="post-link-row">${link}</div>
          ${texts ? `<div class="slide-texts-container">${texts}</div>` : '<div class="no-text">No slide text extracted</div>'}
        </div>
      `;
    }).join('');
    card.classList.remove('hidden');
  },

  async exportCSV() {
    try {
      const res = await fetch('/api/posts/export', { method: 'POST' });
      const blob = await res.blob();
      const cd = res.headers.get('Content-Disposition') || '';
      const match = cd.match(/filename=(.+)/);
      const filename = match ? match[1] : 'posts_export.csv';
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      a.click();
    } catch (e) { console.error(e); }
  },

  esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  },
};

document.addEventListener('DOMContentLoaded', () => App.init());
