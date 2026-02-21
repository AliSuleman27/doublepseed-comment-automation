const ARCHETYPES = {
  personal_testimony:  { label: 'Personal Testimony' },
  situational_react:   { label: 'Situational React' },
  impulsive_action:    { label: 'Impulsive Action' },
  social_sharing:      { label: 'Social Sharing' },
  comparative_real:    { label: 'Comparative Real' },
  hype_validation:     { label: 'Hype Validation' },
  curiosity_question:  { label: 'Curiosity Question' },
  feigned_ignorance:   { label: 'Feigned Ignorance' },
};

const App = {
  templates: {},
  posts: [],
  products: [],
  selectedProduct: null,
  selectedTemplate: null,

  // Auth state
  authToken: null,
  refreshToken: null,
  userEmail: null,

  // Comment engine state
  cePosts: [],
  ceResults: [],
  ceAssignments: [],
  ceRelevanceTags: {},
  ceConfigLoaded: false,
  ceConfigTemplates: [],
  ceDefaultWeights: {},   // from config
  ceAdvancedOpen: false,
  ceSortMode: 'attention',  // 'attention' | 'default' | 'account'
  ceFilterMode: 'all',      // 'all' | 'fallback' | 'flagged' | 'pass'

  async init() {
    console.log('[DS] init — checking auth');
    this.authToken = localStorage.getItem('ds_auth_token');
    this.refreshToken = localStorage.getItem('ds_auth_refresh');

    if (this.authToken) {
      const valid = await this.verifyAuth();
      if (valid) { this.showApp(); return; }
      if (this.refreshToken) {
        const refreshed = await this.tryRefresh();
        if (refreshed) { this.showApp(); return; }
      }
    }
    this.showLogin();
  },

  // ─── Auth ─────────────────────────────
  showLogin() {
    document.getElementById('loginScreen').classList.remove('hidden');
    document.getElementById('appShell').classList.add('hidden');
    document.getElementById('userInfo').classList.add('hidden');
  },

  async showApp() {
    document.getElementById('loginScreen').classList.add('hidden');
    document.getElementById('appShell').classList.remove('hidden');
    document.getElementById('userInfo').classList.remove('hidden');
    document.getElementById('userEmail').textContent = this.userEmail || '';
    await this.initApp();
  },

  async initApp() {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    document.getElementById('startDate').value = this.formatDate(yesterday);
    document.getElementById('endDate').value = this.formatDate(today);
    document.getElementById('ceDate').value = this.formatDate(yesterday);

    await this.loadProducts();

    const lastPid = localStorage.getItem('ds_product');
    const lastTid = localStorage.getItem('ds_template');
    const validPid = this.products.find(p => p.id === lastPid) ? lastPid :
                     (this.products.length > 0 ? this.products[0].id : null);

    if (validPid) {
      await this.selectProduct(validPid);
      if (lastTid) {
        const tsel = document.getElementById('templateSelect');
        if (tsel.querySelector(`option[value="${lastTid}"]`)) {
          tsel.value = lastTid;
          this.selectedTemplate = lastTid;
        }
      }
    }

    this.ceCheckConfig();
  },

  async verifyAuth() {
    try {
      const res = await fetch('/api/auth/me', {
        headers: { 'Authorization': 'Bearer ' + this.authToken }
      });
      if (!res.ok) return false;
      const data = await res.json();
      if (data.user) { this.userEmail = data.user.email; return true; }
      return false;
    } catch (e) { return false; }
  },

  async tryRefresh() {
    try {
      const res = await fetch('/api/auth/refresh', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: this.refreshToken }),
      });
      if (!res.ok) return false;
      const data = await res.json();
      if (data.access_token) {
        this.authToken = data.access_token;
        this.refreshToken = data.refresh_token || this.refreshToken;
        localStorage.setItem('ds_auth_token', this.authToken);
        if (data.refresh_token) localStorage.setItem('ds_auth_refresh', data.refresh_token);
        return await this.verifyAuth();
      }
      return false;
    } catch (e) { return false; }
  },

  async login() {
    const email = document.getElementById('loginEmail').value.trim();
    const password = document.getElementById('loginPassword').value;
    const errorEl = document.getElementById('loginError');
    const btn = document.getElementById('loginBtn');

    if (!email || !password) {
      errorEl.textContent = 'Please enter email and password';
      errorEl.classList.remove('hidden');
      return;
    }

    btn.disabled = true;
    btn.textContent = 'Signing in...';
    errorEl.classList.add('hidden');

    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json();

      if (!res.ok || data.error) {
        errorEl.textContent = data.error || 'Login failed';
        errorEl.classList.remove('hidden');
        btn.disabled = false; btn.textContent = 'Sign In';
        return;
      }

      if (data.access_token) {
        this.authToken = data.access_token;
        this.refreshToken = data.refresh_token;
        this.userEmail = data.user?.email || email;
        localStorage.setItem('ds_auth_token', data.access_token);
        localStorage.setItem('ds_auth_refresh', data.refresh_token);
        this.showApp();
      } else {
        errorEl.textContent = data.message || 'Login failed: no token returned';
        errorEl.classList.remove('hidden');
      }
    } catch (e) {
      errorEl.textContent = 'Connection error: ' + e.message;
      errorEl.classList.remove('hidden');
    }
    btn.disabled = false; btn.textContent = 'Sign In';
  },

  logout() {
    if (this.authToken) {
      fetch('/api/auth/logout', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + this.authToken },
      }).catch(() => {});
    }
    this.authToken = null;
    this.refreshToken = null;
    this.userEmail = null;
    localStorage.removeItem('ds_auth_token');
    localStorage.removeItem('ds_auth_refresh');
    this.showLogin();
  },

  async authFetch(url, options = {}) {
    if (!options.headers) options.headers = {};
    if (this.authToken && !options.headers['Authorization']) {
      options.headers['Authorization'] = 'Bearer ' + this.authToken;
    }
    const res = await fetch(url, options);
    if (res.status === 401 && this.refreshToken) {
      const refreshed = await this.tryRefresh();
      if (refreshed) {
        options.headers['Authorization'] = 'Bearer ' + this.authToken;
        return fetch(url, options);
      }
      this.logout();
      throw new Error('Session expired. Please log in again.');
    }
    return res;
  },

  formatDate(date) { return date.toISOString().split('T')[0]; },

  // ─── Tab Switching ─────────────────────────────
  switchTab(tab) {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    document.getElementById('viewPosts').classList.toggle('hidden', tab !== 'posts');
    document.getElementById('viewComments').classList.toggle('hidden', tab !== 'comments');
    if (tab === 'comments' && this.selectedProduct) {
      document.getElementById('ceProductSelect').value = this.selectedProduct;
      this.ceLoadTemplates(this.selectedProduct);
    }
  },

  // ─── Products (shared) ─────────────────────────
  async loadProducts() {
    this.setStatus('loading');
    try {
      const res = await this.authFetch('/api/products');
      const data = await res.json();
      if (data.error) { this.showWarning(data.error); this.setStatus('error'); return; }
      if (Array.isArray(data) && data.length > 0) {
        this.products = data;
        this.renderSidebar(data);
        this.renderProductDropdown(data);
        this.setStatus('ready');
      } else {
        const testRes = await this.authFetch('/api/test');
        const diag = await testRes.json();
        this.showWarning(diag.ok ? 'No managed clients found.' : diag.message);
        this.setStatus('error');
      }
    } catch (e) {
      this.showWarning('Failed to connect to server: ' + e.message);
      this.setStatus('error');
    }
  },

  renderSidebar(products) {
    const sidebar = document.getElementById('sidebarList');
    const empty = document.getElementById('sidebarEmpty');
    if (empty) empty.remove();
    sidebar.querySelectorAll('.sb-item').forEach(el => el.remove());
    products.forEach(p => {
      const div = document.createElement('div');
      div.className = 'sb-item'; div.dataset.pid = p.id;
      div.onclick = () => App.selectProduct(p.id);
      div.innerHTML = `<span class="icon">◈</span><span>${this.esc(p.title)}</span>`;
      sidebar.appendChild(div);
    });
  },

  renderProductDropdown(products) {
    const opts = '<option value="">Select client...</option>' +
      products.map(p => `<option value="${p.id}">${this.esc(p.title)}</option>`).join('');
    document.getElementById('productSelect').innerHTML = opts;
    document.getElementById('ceProductSelect').innerHTML = opts;
  },

  showWarning(msg) {
    document.getElementById('connWarningMsg').textContent = msg;
    document.getElementById('connWarning').classList.remove('hidden');
  },

  setStatus(state) {
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');
    if (state === 'ready')        { dot.style.background = 'var(--green)';  txt.textContent = 'Ready'; }
    else if (state === 'loading') { dot.style.background = 'var(--orange)'; txt.textContent = 'Loading...'; }
    else if (state === 'error')   { dot.style.background = 'var(--red)';    txt.textContent = 'Error'; }
  },

  async selectProduct(id) {
    if (!id) return;
    this.selectedProduct = id;
    localStorage.setItem('ds_product', id);
    document.querySelectorAll('.sb-item[data-pid]').forEach(el =>
      el.classList.toggle('active', el.dataset.pid === id));
    document.getElementById('productSelect').value = id;
    document.getElementById('ceProductSelect').value = id;
    await this.loadTemplates(id);
    this.ceLoadTemplates(id);
  },

  // ─── Post Viewer: Templates ────────────────────
  async loadTemplates(productId) {
    const tsel = document.getElementById('templateSelect');
    const fetchBtn = document.getElementById('fetchBtn');
    if (this.templates[productId]) { this.renderTemplateDropdown(this.templates[productId]); return; }
    tsel.innerHTML = '<option value="">Loading templates...</option>';
    tsel.disabled = true; fetchBtn.disabled = true;
    try {
      const res = await this.authFetch(`/api/products/${productId}/templates`);
      const data = await res.json();
      if (data.error) { tsel.innerHTML = '<option value="">Error</option>'; return; }
      this.templates[productId] = data;
      this.renderTemplateDropdown(data);
    } catch (e) { tsel.innerHTML = '<option value="">Error</option>'; }
  },

  renderTemplateDropdown(templates) {
    const tsel = document.getElementById('templateSelect');
    const totalPosts = templates.reduce((sum, t) => sum + (t.post_count || 0), 0);
    tsel.innerHTML = '<option value="">All templates (' + templates.length + ') — ' + totalPosts + ' posts</option>' +
      templates.map(t => `<option value="${t.id}">${this.esc(t.title || 'Template ' + t.id.slice(0, 8))}</option>`).join('');
    tsel.disabled = false;
    document.getElementById('fetchBtn').disabled = false;
  },

  onProductChange(id) { if (id) this.selectProduct(id); },
  onTemplateChange() {
    const tid = document.getElementById('templateSelect').value || null;
    this.selectedTemplate = tid;
    if (tid) localStorage.setItem('ds_template', tid); else localStorage.removeItem('ds_template');
  },

  // ─── Post Viewer: Fetch & Render ──────────────
  async fetchPosts() {
    const pid = this.selectedProduct; if (!pid) return;
    const tid = document.getElementById('templateSelect').value || null;
    const startDate = document.getElementById('startDate').value || null;
    const endDate = document.getElementById('endDate').value || null;
    const postFilter = document.getElementById('postFilter').value;
    const statusFilter = document.getElementById('statusFilter').value;
    const statuses = statusFilter.split(',');

    document.getElementById('fetchBtn').disabled = true;
    document.getElementById('fetchStatus').textContent = 'Fetching...';
    this.setStatus('loading');
    try {
      const body = { product_id: pid, template_id: tid, statuses };
      if (startDate) body.start_date = startDate + 'T00:00:00Z';
      if (endDate) body.end_date = endDate + 'T23:59:59Z';
      const res = await this.authFetch('/api/posts/fetch', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const data = await res.json();
      if (data.error) { document.getElementById('fetchStatus').textContent = data.error; this.setStatus('error'); return; }

      let posts = data.posts;
      const totalFetched = posts.length;
      if (postFilter === 'with_link') {
        posts = posts.filter(p => p.tiktok_post_id);
      }
      this.posts = posts;
      this.renderStats({ posts, count: posts.length });
      this.renderPosts(posts);
      const filterNote = postFilter === 'with_link' && totalFetched !== posts.length ? ` (${totalFetched - posts.length} without link skipped)` : '';
      document.getElementById('fetchStatus').textContent = `${posts.length} posts found${filterNote}`;
      document.getElementById('exportBtn').classList.toggle('hidden', posts.length === 0);
      this.setStatus('ready');
    } catch (e) { document.getElementById('fetchStatus').textContent = 'Error: ' + e.message; this.setStatus('error'); }
    finally { document.getElementById('fetchBtn').disabled = false; }
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
    document.getElementById('postsSub').textContent = `${posts.length} slideshow posts`;
    if (!posts.length) {
      container.innerHTML = '<div style="padding:24px; text-align:center; color:var(--text-3);">No posts found.</div>';
      card.classList.remove('hidden'); return;
    }
    container.innerHTML = posts.map((p, i) => {
      const texts = (p.slide_texts || []).map((t, j) => `<div class="slide-text"><span class="slide-num">S${j+1}</span>${this.esc(t)}</div>`).join('');
      const link = p.tiktok_url ? `<a href="${p.tiktok_url}" target="_blank" class="post-link">${p.tiktok_url}</a>` : '<span style="color:var(--text-3);">No link</span>';
      const time = p.created_at ? new Date(p.created_at).toLocaleString() : '—';
      const statusClass = p.status === 'scheduled' ? 'post-status-scheduled' : '';
      const statusLabel = p.status || 'succeeded';
      return `<div class="post-row fade-in" style="animation-delay:${Math.min(i*20,400)}ms">
        <div class="post-header"><div class="post-acct">@${this.esc(p.account_username)}</div><div class="post-meta">${p.num_slides||'?'} slides · ${time}</div></div>
        <div>${p.template_name ? `<span class="post-template">${this.esc(p.template_name)}</span>` : ''}<span class="post-type">slideshow</span><span class="post-type ${statusClass}" style="${statusLabel === 'succeeded' ? 'color:var(--green);background:var(--green-dim);' : ''}">${statusLabel}</span></div>
        ${p.hook ? `<div class="post-hook">${this.esc(p.hook)}</div>` : ''}
        <div class="post-caption">${this.esc(p.caption||p.title||'')}</div>
        <div class="post-link-row">${link}</div>
        ${texts ? `<div class="slide-texts-container">${texts}</div>` : '<div class="no-text">No slide text</div>'}
      </div>`;
    }).join('');
    card.classList.remove('hidden');
  },

  async exportCSV() {
    try {
      const res = await this.authFetch('/api/posts/export', { method: 'POST' });
      const blob = await res.blob();
      const cd = res.headers.get('Content-Disposition') || '';
      const match = cd.match(/filename=(.+)/);
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
      a.download = match ? match[1] : 'posts_export.csv'; a.click();
    } catch (e) { console.error(e); }
  },

  // ═══════════════════════════════════════════════
  // COMMENT ENGINE
  // ═══════════════════════════════════════════════

  // ─── Config Upload ────────────────────────────
  async ceCheckConfig() {
    try {
      const res = await this.authFetch('/api/ce/config');
      const data = await res.json();
      if (data.loaded) {
        this.ceConfigLoaded = true;
        this.ceConfigTemplates = data.templates || [];
        document.getElementById('ceConfigStatus').textContent = `${data.brand} — ${data.templates.join(', ')}`;
        document.getElementById('ceConfigStatus').style.color = 'var(--green)';
        document.getElementById('ceConfigInfo').textContent = 'Config loaded on server';
        document.getElementById('ceAdvancedCard').classList.remove('hidden');
        this.ceFetchConfigDetail();
      }
    } catch (e) { /* ignore */ }
  },

  async ceUploadConfig(input) {
    const file = input.files[0];
    if (!file) return;
    const info = document.getElementById('ceConfigInfo');
    info.textContent = 'Uploading...';

    const formData = new FormData();
    formData.append('config', file);

    try {
      const res = await this.authFetch('/api/ce/config', { method: 'POST', body: formData });
      const data = await res.json();
      if (data.error) { info.textContent = 'Error: ' + data.error; info.style.color = 'var(--red)'; return; }

      this.ceConfigLoaded = true;
      this.ceConfigTemplates = data.templates || [];
      document.getElementById('ceConfigStatus').textContent = `${data.brand} — ${data.templates.join(', ')}`;
      document.getElementById('ceConfigStatus').style.color = 'var(--green)';
      info.textContent = `Loaded: ${data.brand} with ${data.templates.length} template(s)`;
      info.style.color = 'var(--green)';

      // Show advanced controls and fetch config detail
      document.getElementById('ceAdvancedCard').classList.remove('hidden');
      this.ceFetchConfigDetail();
    } catch (e) {
      info.textContent = 'Upload failed: ' + e.message;
      info.style.color = 'var(--red)';
    }
    input.value = '';
  },

  // ─── CE Product/Template ──────────────────────
  ceOnProductChange(id) {
    if (!id) return;
    this.selectedProduct = id;
    localStorage.setItem('ds_product', id);
    document.querySelectorAll('.sb-item[data-pid]').forEach(el =>
      el.classList.toggle('active', el.dataset.pid === id));
    document.getElementById('productSelect').value = id;
    this.ceLoadTemplates(id);
  },

  async ceLoadTemplates(productId) {
    const tsel = document.getElementById('ceTemplateSelect');
    if (!productId) return;
    if (this.templates[productId]) { this.ceRenderTemplateDropdown(this.templates[productId]); return; }
    tsel.innerHTML = '<option value="">Loading...</option>'; tsel.disabled = true;
    try {
      const res = await this.authFetch(`/api/products/${productId}/templates`);
      const data = await res.json();
      if (data.error) { tsel.innerHTML = '<option value="">Error</option>'; return; }
      this.templates[productId] = data;
      this.ceRenderTemplateDropdown(data);
    } catch (e) { tsel.innerHTML = '<option value="">Error</option>'; }
  },

  ceRenderTemplateDropdown(templates) {
    const tsel = document.getElementById('ceTemplateSelect');
    tsel.innerHTML = '<option value="">Select template...</option>' +
      templates.map(t => `<option value="${t.id}" data-title="${this.esc(t.title || '')}">${this.esc(t.title || 'Template ' + t.id.slice(0,8))}</option>`).join('');
    tsel.disabled = false;
    document.getElementById('ceLoadBtn').disabled = false;
  },

  ceOnTemplateChange() {
    // Re-fetch config detail for the new template
    if (this.ceConfigLoaded) this.ceFetchConfigDetail();
  },

  // ─── Advanced Controls ──────────────────────
  ceToggleAdvanced() {
    this.ceAdvancedOpen = !this.ceAdvancedOpen;
    document.getElementById('ceAdvancedBody').classList.toggle('hidden', !this.ceAdvancedOpen);
    document.getElementById('ceAdvancedToggle').textContent = this.ceAdvancedOpen ? 'Hide' : 'Show';
  },

  async ceFetchConfigDetail() {
    try {
      const tsel = document.getElementById('ceTemplateSelect');
      const selectedOpt = tsel.selectedOptions[0];
      const templateTitle = selectedOpt ? selectedOpt.dataset.title || selectedOpt.textContent : '';
      let templateSlug = '';
      for (const slug of this.ceConfigTemplates) {
        const slugLower = slug.toLowerCase().replace(/[\s-_]/g, '');
        const titleLower = templateTitle.toLowerCase().replace(/[\s-_]/g, '');
        if (titleLower.includes(slugLower) || slugLower.includes(titleLower.substring(0, 3))) {
          templateSlug = slug; break;
        }
      }
      if (!templateSlug && this.ceConfigTemplates.length > 0) templateSlug = this.ceConfigTemplates[0];

      const res = await this.authFetch(`/api/ce/config/detail?template_slug=${encodeURIComponent(templateSlug)}`);
      const data = await res.json();
      if (!data.loaded) return;

      // Populate archetype weight sliders
      this.ceDefaultWeights = data.archetype_weights || {};
      this.ceRenderArchSliders(this.ceDefaultWeights);

      // Set relevance ratio from config
      const relRatio = Math.round((data.relevance_ratio || 0.5) * 100);
      document.getElementById('ceRelevance').value = relRatio;
      document.getElementById('ceRelevanceVal').textContent = relRatio + '%';

      // Set word count from config
      const rules = data.comment_rules || {};
      const [wmin, wmax] = rules.word_count_range || [6, 18];
      document.getElementById('ceMinWords').value = wmin;
      document.getElementById('ceMaxWords').value = wmax;
    } catch (e) { console.warn('[CE] Config detail fetch error:', e); }
  },

  ceRenderArchSliders(weights) {
    const container = document.getElementById('ceArchSliders');
    const colors = {
      personal_testimony: '#0080ff', situational_react: '#00c758',
      impulsive_action: '#fe6e00', social_sharing: '#ac4bff',
      comparative_real: '#edb200', hype_validation: '#3dd68c',
      curiosity_question: '#36d6e7', feigned_ignorance: '#ff5c8a',
    };
    let html = '';
    for (const [arch, weight] of Object.entries(weights)) {
      const pct = Math.round(weight * 100);
      const label = (ARCHETYPES[arch] || {}).label || arch;
      const color = colors[arch] || '#666';
      html += `
        <div class="ce-arch-slider-row">
          <div class="ce-arch-slider-label">
            <span class="arch-dot" style="background:${color}"></span>
            ${label}
          </div>
          <input type="range" class="ce-arch-slider-input" data-arch="${arch}"
                 min="0" max="100" value="${pct}"
                 oninput="App.ceUpdateArchPct('${arch}', this.value)">
          <span class="ce-arch-slider-pct" id="ceArchPct_${arch}">${pct}%</span>
        </div>`;
    }
    container.innerHTML = html;
  },

  ceUpdateArchPct(arch, val) {
    const el = document.getElementById(`ceArchPct_${arch}`);
    if (el) el.textContent = val + '%';
  },

  ceResetWeights() {
    this.ceRenderArchSliders(this.ceDefaultWeights);
  },

  ceGetOverrides() {
    const overrides = {};

    // Archetype weights
    const sliders = document.querySelectorAll('.ce-arch-slider-input');
    if (sliders.length > 0) {
      const weights = {};
      sliders.forEach(s => { weights[s.dataset.arch] = parseInt(s.value) / 100; });
      overrides.archetype_weights = weights;
    }

    // Relevance ratio
    overrides.relevance_ratio = parseInt(document.getElementById('ceRelevance').value) / 100;

    // Temperature
    overrides.temperature = parseInt(document.getElementById('ceTemperature').value) / 100;

    // Slang frequency
    overrides.slang_frequency = document.getElementById('ceSlang').value;

    // Brand casing
    const casingVal = document.getElementById('ceBrandCasing').value;
    if (casingVal === 'clickup_only') {
      overrides.brand_casing = { "clickup": 1.0 };
    } else if (casingVal === 'Clickup_only') {
      overrides.brand_casing = { "Clickup": 1.0 };
    } else if (casingVal === 'mixed') {
      overrides.brand_casing = { "clickup": 0.5, "Clickup": 0.5 };
    }
    // 'config' = no override, use config defaults

    // Max per structure
    overrides.max_per_structure = parseInt(document.getElementById('ceMaxStructure').value);

    // Word count overrides
    overrides.word_count_min = parseInt(document.getElementById('ceMinWords').value);
    overrides.word_count_max = parseInt(document.getElementById('ceMaxWords').value);

    return overrides;
  },

  // ─── Load Posts ────────────────────────────────
  async ceLoadPosts() {
    const pid = document.getElementById('ceProductSelect').value;
    const tid = document.getElementById('ceTemplateSelect').value || null;
    const date = document.getElementById('ceDate').value;
    const ceFilter = document.getElementById('cePostFilter').value;
    const statusFilter = document.getElementById('ceStatusFilter').value;
    const statuses = statusFilter.split(',');
    if (!pid) return;

    document.getElementById('ceLoadBtn').disabled = true;
    document.getElementById('ceLoadStatus').textContent = 'Fetching posts...';
    this.setStatus('loading');

    try {
      const body = { product_id: pid, template_id: tid, statuses };
      if (date) { body.start_date = date + 'T00:00:00Z'; body.end_date = date + 'T23:59:59Z'; }

      const res = await this.authFetch('/api/posts/fetch', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const data = await res.json();
      if (data.error) { document.getElementById('ceLoadStatus').textContent = data.error; this.setStatus('error'); return; }

      const total = data.posts.length;
      if (ceFilter === 'with_link') {
        this.cePosts = data.posts.filter(p => p.tiktok_post_id);
      } else {
        this.cePosts = data.posts;
      }
      this.ceResults = [];
      this.ceAssignments = [];

      this.ceSetStage('select');
      this.ceRenderStats();
      this.ceRenderPosts();

      const linked = this.cePosts.length;
      const filterNote = ceFilter === 'with_link' && total !== linked ? ` (${total - linked} without link skipped)` : '';
      document.getElementById('ceLoadStatus').textContent = `${linked} posts loaded${filterNote}`;
      this.setStatus('ready');

      document.getElementById('ceBatchActions').classList.remove('hidden');
      const batchSize = parseInt(document.getElementById('ceBatchSize').value);
      const batches = Math.ceil(linked / batchSize);
      document.getElementById('ceBatchSub').textContent = `${linked} posts · ${batches} batch${batches!==1?'es':''} of ${batchSize}`;
    } catch (e) {
      document.getElementById('ceLoadStatus').textContent = 'Error: ' + e.message;
      this.setStatus('error');
    } finally { document.getElementById('ceLoadBtn').disabled = false; }
  },

  ceSetStage(stage) {
    const stages = ['select', 'archetype', 'generate', 'validate', 'review', 'export'];
    const idx = stages.indexOf(stage);
    document.querySelectorAll('.pipe-stage').forEach((el, i) => {
      el.classList.remove('active', 'done');
      if (i < idx) el.classList.add('done');
      else if (i === idx) el.classList.add('active');
    });
  },

  ceRenderStats() {
    const row = document.getElementById('ceStatsRow');
    const posts = this.cePosts;
    const accounts = new Set(posts.map(p => p.account_username));
    const batchSize = parseInt(document.getElementById('ceBatchSize').value);
    const batches = Math.ceil(posts.length / batchSize);
    const model = document.getElementById('ceModel').selectedOptions[0].text;
    row.innerHTML = `
      <div class="stat"><div class="stat-val">${posts.length}</div><div class="stat-lbl">Posts</div></div>
      <div class="stat"><div class="stat-val">${accounts.size}</div><div class="stat-lbl">Accounts</div></div>
      <div class="stat"><div class="stat-val">${batches}</div><div class="stat-lbl">Batches</div></div>
      <div class="stat"><div class="stat-val">~${(batches*2200).toLocaleString()}</div><div class="stat-lbl">Est. Tokens</div></div>
      <div class="stat"><div class="stat-val">${model}</div><div class="stat-lbl">Model</div></div>
    `;
    row.classList.remove('hidden');
  },

  // ─── Run Full Pipeline (batch-by-batch streaming) ────────────────
  async ceRunPipeline() {
    if (!this.ceConfigLoaded) {
      document.getElementById('cePipelineStatus').textContent = 'Upload a brand config first!';
      document.getElementById('cePipelineStatus').style.color = 'var(--red)';
      return;
    }
    if (!this.cePosts.length) return;

    const model = document.getElementById('ceModel').value;
    const batchSize = parseInt(document.getElementById('ceBatchSize').value);
    const tsel = document.getElementById('ceTemplateSelect');
    const selectedOpt = tsel.selectedOptions[0];
    const templateTitle = selectedOpt ? selectedOpt.dataset.title || selectedOpt.textContent : '';

    // Try to match template slug from config
    let templateSlug = '';
    for (const slug of this.ceConfigTemplates) {
      const slugLower = slug.toLowerCase().replace(/[\s-_]/g, '');
      const titleLower = templateTitle.toLowerCase().replace(/[\s-_]/g, '');
      if (titleLower.includes(slugLower) || slugLower.includes(titleLower.substring(0, 3))) {
        templateSlug = slug;
        break;
      }
    }
    if (!templateSlug && this.ceConfigTemplates.length > 0) {
      templateSlug = this.ceConfigTemplates[0];
    }

    const btn = document.getElementById('ceRunPipelineBtn');
    const status = document.getElementById('cePipelineStatus');
    btn.disabled = true;
    btn.textContent = 'Running pipeline...';
    btn.classList.add('generating');

    // Delay between batches (ms) — avoids rate limiting
    const BATCH_DELAY = model === 'gemini-flash' ? 3000 : 1500;

    // Switch to Comment Engine tab and scroll to pipeline area
    this.switchTab('comments');
    document.getElementById('ceBatchActions').scrollIntoView({ behavior: 'smooth', block: 'start' });

    // ─── Step 1: Prepare pipeline (assign archetypes, build prompts) ───
    status.textContent = 'Preparing pipeline — assigning archetypes...';
    status.style.color = 'var(--orange)';
    this.ceSetStage('archetype');

    // Collect UI overrides
    const overrides = this.ceGetOverrides();
    console.log('[CE] Overrides:', overrides);

    let prepData;
    try {
      const prepRes = await this.authFetch('/api/ce/prepare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          posts: this.cePosts,
          template_slug: templateSlug,
          model: model,
          batch_size: batchSize,
          overrides: overrides,
        }),
      });
      prepData = await prepRes.json();
      if (prepData.error) {
        status.textContent = 'Prepare error: ' + prepData.error;
        status.style.color = 'var(--red)';
        this.ceSetStage('select');
        btn.disabled = false; btn.textContent = 'Run Full Pipeline'; btn.classList.remove('generating');
        return;
      }
    } catch (e) {
      status.textContent = 'Prepare error: ' + e.message;
      status.style.color = 'var(--red)';
      this.ceSetStage('select');
      btn.disabled = false; btn.textContent = 'Run Full Pipeline'; btn.classList.remove('generating');
      return;
    }

    const totalBatches = prepData.total_batches;
    this.ceAssignments = prepData.assignments || [];
    this.ceRelevanceTags = prepData.relevance_tags || {};
    this.ceResults = [];

    // Build assignment map for rendering
    const assignMap = {};
    this.ceAssignments.forEach(a => { assignMap[a.post_id] = a; });

    // Render posts with archetypes (before LLM calls)
    this.ceRenderPostsPrePipeline(assignMap);

    // ─── Step 2: Process batches one by one ───
    this.ceSetStage('generate');
    let totalPass = 0, totalFlagged = 0, totalFallback = 0;
    const allErrors = [];

    for (let bi = 0; bi < totalBatches; bi++) {
      // Delay between batches to avoid rate limiting
      if (bi > 0) {
        const delaySec = Math.round(BATCH_DELAY / 1000);
        status.textContent = `Waiting ${delaySec}s before batch ${bi + 1}...`;
        await new Promise(r => setTimeout(r, BATCH_DELAY));
      }

      status.textContent = `Generating batch ${bi + 1} of ${totalBatches}...`;

      // Highlight current batch cards
      this._ceHighlightBatch(bi, batchSize, true);

      try {
        const batchRes = await this.authFetch(`/api/ce/batch/${bi}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        });
        const batchData = await batchRes.json();

        if (batchData.error) {
          allErrors.push(batchData.error);
          this._ceHighlightBatch(bi, batchSize, false);
          continue;
        }

        // Accumulate results
        this.ceResults.push(...(batchData.results || []));
        const bs = batchData.batch_summary || {};
        totalPass += bs.llm_pass || 0;
        totalFlagged += bs.flagged || 0;
        totalFallback += bs.fallback_used || 0;

        if (batchData.error) allErrors.push(batchData.error);

        // Render this batch's results immediately
        this._ceRenderBatchResults(bi, batchSize, batchData.results, assignMap);

      } catch (e) {
        allErrors.push(`Batch ${bi + 1}: ${e.message}`);
      }

      this._ceHighlightBatch(bi, batchSize, false);
    }

    // ─── Step 3: Show summary ───
    this.ceSetStage('review');
    const summary = {
      total_posts: this.cePosts.length,
      total_comments: this.ceResults.length,
      llm_pass: totalPass,
      flagged: totalFlagged,
      fallback_used: totalFallback,
      batches: totalBatches,
      model: model,
      errors: allErrors,
    };

    this.ceRenderSummary(summary);
    status.textContent = `Done! ${totalPass} passed, ${totalFlagged} flagged, ${totalFallback} fallbacks`;
    status.style.color = 'var(--green)';
    document.getElementById('ceExportBtn').classList.remove('hidden');

    // Show sort/filter bar and default to "needs attention" sort
    document.getElementById('ceSortFilterBar').classList.remove('hidden');
    this.ceSortMode = 'attention';
    this.ceFilterMode = 'all';
    this.ceReRenderAllResults();

    btn.disabled = false;
    btn.textContent = 'Run Full Pipeline';
    btn.classList.remove('generating');
  },

  // ─── Test Pipeline (mock data, no LLM) ────────────────
  ceRunTestPipeline() {
    if (!this.cePosts.length) {
      const status = document.getElementById('cePipelineStatus');
      status.textContent = 'Load posts first!';
      status.style.color = 'var(--red)';
      return;
    }

    const archetypeKeys = Object.keys(ARCHETYPES);
    const mockComments = [
      "clickup literally saved my workflow last week ngl",
      "wait this is actually how I plan my entire week now",
      "my manager would lose it if she saw how organized this is",
      "ok but why does this look exactly like my to-do list rn",
      "been using this for 3 months and honestly can't go back",
      "this is giving productive era and I'm here for it",
      "someone tell my coworker about this before I lose my mind",
      "the way I just reorganized my whole life in 20 minutes",
      "not me watching this at 2am instead of actually being productive",
      "I showed this to my team and now everyone's obsessed",
      "can someone drop the app name bc I need this immediately",
      "what app is that I keep seeing it everywhere",
      "this the kind of organization I pretend to have",
      "lowkey need this for my side project asap",
      "my brain just went from chaos to clarity watching this",
      "Clickup making project management look effortless honestly",
      "why did nobody tell me about this sooner genuinely asking",
      "this is exactly what our team needed for sprint planning",
      "the dashboard view alone is worth it not even joking",
      "ok I'm convinced where do I sign up",
      "this gave me the motivation to finally organize my tasks",
      "been looking for something like this for months wow",
      "the way this just simplified my entire morning routine",
      "clickup really said let me fix your whole workflow huh",
    ];

    const flaggedComments = [
      "OMG this is literally the BEST app I've ever seen!!!",
      "clickup is honestly just so amazing and wonderful and great for productivity",
      "this tool is a game changer for real like no cap it changed everything",
      "I need this. I want this. I have to get this app right now.",
    ];

    const fallbackComments = [
      "can someone tell me what app this is",
      "what's the app at the end asking for a friend",
      "ok but drop the link please",
      "this is the energy I needed today honestly",
    ];

    const passChecks = [
      { label: 'No emoji', status: 'pass' },
      { label: 'No hashtags', status: 'pass' },
      { label: 'No ad language', status: 'pass' },
      { label: 'No banned patterns', status: 'pass' },
    ];

    const posts = this.cePosts;
    const assignments = [];
    const relevanceTags = {};
    const results = [];

    posts.forEach((p, i) => {
      const arch = archetypeKeys[Math.floor(Math.random() * archetypeKeys.length)];
      const brandMention = arch !== 'feigned_ignorance' && Math.random() > 0.3;
      const relTag = Math.random() > 0.5 ? 'specific' : 'vibe';

      assignments.push({ post_id: p.id, archetype: arch, brand_mention: brandMention });
      relevanceTags[p.id] = relTag;

      // Decide status: ~60% pass, ~20% flagged, ~20% fallback
      const roll = Math.random();
      let status, source, comment, checks;

      if (roll < 0.6) {
        status = 'pass';
        source = 'llm';
        comment = mockComments[Math.floor(Math.random() * mockComments.length)];
        const wc = comment.split(/\s+/).length;
        checks = [
          ...passChecks,
          { label: `${wc} words`, status: 'pass' },
          { label: brandMention ? 'Brand mentioned' : 'No brand (mystery)', status: 'pass' },
        ];
      } else if (roll < 0.8) {
        status = 'flagged';
        source = 'llm';
        comment = flaggedComments[Math.floor(Math.random() * flaggedComments.length)];
        const wc = comment.split(/\s+/).length;
        const warnType = Math.random() > 0.5
          ? { label: 'Excessive caps', status: 'warn' }
          : { label: `${wc} words`, status: 'warn' };
        checks = [
          ...passChecks,
          warnType,
          { label: brandMention ? 'Brand mentioned' : 'No brand (mystery)', status: 'pass' },
        ];
      } else {
        status = 'fallback';
        source = 'fallback';
        comment = fallbackComments[Math.floor(Math.random() * fallbackComments.length)];
        const wc = comment.split(/\s+/).length;
        const failType = Math.random() > 0.5
          ? { label: 'Duplicate (sim=0.72)', status: 'fail' }
          : { label: 'No banned patterns', status: 'fail' };
        checks = [
          { label: 'No emoji', status: 'pass' },
          { label: 'No hashtags', status: 'pass' },
          failType,
          { label: `${wc} words`, status: 'pass' },
        ];
      }

      results.push({
        post_id: p.id,
        account_username: p.account_username,
        tiktok_url: p.tiktok_url || '',
        archetype: arch,
        brand_mention: brandMention,
        comment,
        word_count: comment.split(/\s+/).length,
        source,
        status,
        checks,
      });
    });

    // Store state
    this.ceAssignments = assignments;
    this.ceRelevanceTags = relevanceTags;
    this.ceResults = results;

    // Build assignment map
    const assignMap = {};
    assignments.forEach(a => { assignMap[a.post_id] = a; });

    // Render posts with archetypes + comments
    this.ceRenderPostsPrePipeline(assignMap);

    // Now render each result into its card
    const resultMap = {};
    results.forEach(r => { resultMap[r.post_id] = r; });

    posts.forEach((p, i) => {
      const result = resultMap[p.id];
      if (!result) return;
      const card = document.getElementById(`ce-post-${i}`);
      if (!card) return;

      card.classList.add('has-comment');
      const commentBox = card.querySelector('.ce-comment-box');
      if (!commentBox) return;

      const statusColor = result.status === 'pass' ? 'var(--green)' : result.status === 'fallback' ? 'var(--red)' : 'var(--orange)';
      const boxClass = result.status === 'pass' ? 'valid' : result.status === 'fallback' ? 'invalid' : 'valid';
      commentBox.className = `ce-comment-box ${boxClass}`;

      const checksHtml = (result.checks || []).map(c => `<span class="val-badge val-${c.status}">${c.label}</span>`).join('');
      const sourceHtml = `<span class="val-badge ${result.source === 'llm' ? 'val-pass' : 'val-fail'}">${result.source}</span>`;

      commentBox.innerHTML = `
        <div class="ce-comment-label">
          <span>Generated Comment (${result.word_count} words)</span>
          <span style="font-size:10px; color:${statusColor}; text-transform:uppercase; font-weight:700;">${result.status}</span>
        </div>
        <div class="ce-comment-text" id="ce-comment-text-${i}">"${this.esc(result.comment)}"</div>
        <div class="ce-comment-wc" id="ce-comment-wc-${i}">${result.word_count} words</div>
        <div class="ce-comment-actions">
          <button class="ce-edit-btn" onclick="App.ceEditComment(${i})">Edit</button>
          ${sourceHtml}
        </div>
        ${checksHtml ? `<div class="validation-row">${checksHtml}</div>` : ''}
      `;
    });

    // Summary
    const totalPass = results.filter(r => r.status === 'pass').length;
    const totalFlagged = results.filter(r => r.status === 'flagged').length;
    const totalFallback = results.filter(r => r.status === 'fallback').length;

    this.ceRenderSummary({
      total_posts: posts.length,
      total_comments: results.length,
      llm_pass: totalPass,
      flagged: totalFlagged,
      fallback_used: totalFallback,
      batches: 1,
      model: 'test-mock',
      errors: [],
    });

    this.ceSetStage('review');
    const status = document.getElementById('cePipelineStatus');
    status.textContent = `Test done! ${totalPass} passed, ${totalFlagged} flagged, ${totalFallback} fallbacks`;
    status.style.color = 'var(--green)';
    document.getElementById('ceExportBtn').classList.remove('hidden');

    // Show sort/filter bar
    document.getElementById('ceSortFilterBar').classList.remove('hidden');
    this.ceSortMode = 'attention';
    this.ceFilterMode = 'all';
    this.ceReRenderAllResults();
  },

  _ceHighlightBatch(batchIdx, batchSize, on) {
    const start = batchIdx * batchSize;
    const end = Math.min(start + batchSize, this.cePosts.length);
    for (let i = start; i < end; i++) {
      const card = document.getElementById(`ce-post-${i}`);
      if (card) card.classList.toggle('batch-processing', on);
    }
  },

  _ceRenderBatchResults(batchIdx, batchSize, results, assignMap) {
    const resultMap = {};
    results.forEach(r => { resultMap[r.post_id] = r; });

    const start = batchIdx * batchSize;
    const end = Math.min(start + batchSize, this.cePosts.length);

    for (let i = start; i < end; i++) {
      const p = this.cePosts[i];
      const result = resultMap[p.id];
      const assign = assignMap[p.id];
      if (!result) continue;

      const card = document.getElementById(`ce-post-${i}`);
      if (!card) continue;

      card.classList.add('has-comment');

      // Update comment box
      const commentBox = card.querySelector('.ce-comment-box');
      if (commentBox) {
        const statusColor = result.status === 'pass' ? 'var(--green)' : result.status === 'fallback' ? 'var(--red)' : result.status === 'flagged' ? 'var(--orange)' : 'var(--text-3)';
        const boxClass = result.status === 'pass' ? 'valid' : result.status === 'fallback' ? 'invalid' : result.status === 'flagged' ? 'valid' : 'pending';
        commentBox.className = `ce-comment-box ${boxClass}`;

        const checks = (result.checks || []).map(c => `<span class="val-badge val-${c.status}">${c.label}</span>`).join('');
        const source = result.source ? `<span class="val-badge ${result.source === 'llm' ? 'val-pass' : 'val-fail'}">${result.source}</span>` : '';

        commentBox.innerHTML = `
          <div class="ce-comment-label">
            <span>Generated Comment (${result.word_count} words)</span>
            <span style="font-size:10px; color:${statusColor}; text-transform:uppercase; font-weight:700;">${result.status}</span>
          </div>
          <div class="ce-comment-text" id="ce-comment-text-${i}">"${this.esc(result.comment)}"</div>
          <div class="ce-comment-wc" id="ce-comment-wc-${i}">${result.word_count} words</div>
          <div class="ce-comment-actions">
            <button class="ce-edit-btn" onclick="App.ceEditComment(${i})">Edit</button>
            ${source}
          </div>
          ${checks ? `<div class="validation-row">${checks}</div>` : ''}
        `;
      }

      // Update archetype row (add source badge)
      const archRow = card.querySelector('.archetype-row');
      if (archRow && assign) {
        archRow.innerHTML = `
          <span class="archetype-badge arch-${assign.archetype}">${(ARCHETYPES[assign.archetype]||{}).label||assign.archetype}</span>
          <span class="brand-mention-badge ${assign.brand_mention ? 'yes' : ''}">${assign.brand_mention ? 'Mention brand' : 'No brand mention'}</span>
        `;
      }

      // Scroll the card into view for the first card in the batch
      if (i === start) {
        card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    }
  },

  ceRenderSummary(summary) {
    const card = document.getElementById('ceSummaryCard');
    const content = document.getElementById('ceSummaryContent');
    const errors = (summary.errors || []);
    content.innerHTML = `
      <div class="g5" style="margin-bottom:${errors.length ? '12px' : '0'}">
        <div class="stat"><div class="stat-val">${summary.total_comments || 0}</div><div class="stat-lbl">Comments</div></div>
        <div class="stat"><div class="stat-val" style="color:var(--green)">${summary.llm_pass || 0}</div><div class="stat-lbl">LLM Pass</div></div>
        <div class="stat"><div class="stat-val" style="color:var(--orange)">${summary.flagged || 0}</div><div class="stat-lbl">Flagged</div></div>
        <div class="stat"><div class="stat-val" style="color:var(--red)">${summary.fallback_used || 0}</div><div class="stat-lbl">Fallbacks</div></div>
        <div class="stat"><div class="stat-val">${summary.batches || 0}</div><div class="stat-lbl">Batches</div></div>
      </div>
      ${errors.length ? `<div style="font-size:12px; color:var(--red); margin-top:8px;">${errors.map(e => this.esc(e)).join('<br>')}</div>` : ''}
    `;
    card.classList.remove('hidden');
  },

  ceFinishReview() {
    this.ceSetStage('export');
  },

  // ─── Render Posts (before pipeline — with archetypes) ───────────
  ceRenderPostsPrePipeline(assignMap) {
    const container = document.getElementById('cePostsList');
    const posts = this.cePosts;
    const batchSize = parseInt(document.getElementById('ceBatchSize').value);

    if (!posts.length) {
      container.innerHTML = '<div class="card" style="text-align:center; color:var(--text-3); padding:40px;">No posts found matching filter.</div>';
      container.classList.remove('hidden'); return;
    }

    let html = '';
    posts.forEach((p, i) => {
      if (i % batchSize === 0) {
        const batchNum = Math.floor(i / batchSize) + 1;
        html += `<div class="batch-divider"><div class="batch-divider-line"></div><div class="batch-divider-label">BATCH ${batchNum} · Posts ${i+1}–${Math.min(i+batchSize, posts.length)}</div><div class="batch-divider-line"></div></div>`;
      }

      const assign = assignMap ? assignMap[p.id] : null;
      const arch = assign ? assign.archetype : '';
      const brandMention = assign ? assign.brand_mention : true;
      const relTag = this.ceRelevanceTags[p.id] || '';

      const slides = (p.slide_texts||[]).map((t,j) => `<div class="slide-text"><span class="slide-num">S${j+1}</span>${this.esc(t)}</div>`).join('');
      const statusLabel = p.status || 'succeeded';
      const statusClass = statusLabel === 'scheduled' ? 'post-status-scheduled' : '';

      html += `
        <div class="ce-post-card fade-in" id="ce-post-${i}" style="animation-delay:${Math.min(i*30,500)}ms">
          <div class="ce-post-top">
            <div class="ce-post-info">
              <div class="ce-post-num">POST ${i+1}</div>
              <div class="ce-post-acct">@${this.esc(p.account_username)}</div>
              ${p.template_name ? `<span class="post-template" style="margin-top:4px">${this.esc(p.template_name)}</span>` : ''}
              <span class="post-type ${statusClass}" style="${statusLabel === 'succeeded' ? 'color:var(--green);background:var(--green-dim);' : ''} margin-top:4px">${statusLabel}</span>
            </div>
            ${p.tiktok_url
              ? `<a href="${p.tiktok_url}" target="_blank" class="ce-post-link">${p.tiktok_url}</a>`
              : '<span style="font-size:11px; color:var(--text-3);">No TikTok link</span>'}
          </div>
          ${p.hook ? `<div class="ce-post-hook">${this.esc(p.hook)}</div>` : ''}
          ${slides ? `<div class="ce-post-slides">${slides}</div>` : ''}
          <div class="archetype-row">
            ${arch ? `<span class="archetype-badge arch-${arch}">${(ARCHETYPES[arch]||{}).label||arch}</span>` : ''}
            ${assign ? `<span class="brand-mention-badge ${brandMention ? 'yes' : ''}">${brandMention ? 'Mention brand' : 'No brand mention'}</span>` : '<span style="font-size:12px; color:var(--text-3);">Awaiting pipeline...</span>'}
            ${relTag ? `<span class="relevance-badge ${relTag}">${relTag}</span>` : ''}
          </div>
          <div class="ce-comment-box pending">
            <div class="ce-comment-label"><span>Generated Comment</span></div>
            <div class="ce-comment-placeholder">Waiting for LLM...</div>
          </div>
        </div>`;
    });
    container.innerHTML = html;
    container.classList.remove('hidden');
  },

  // ─── Render Posts (initial load, no pipeline yet) ───────────
  ceRenderPosts() {
    this.ceRenderPostsPrePipeline(null);
  },

  // ─── Edit Comment ──────────────────────────────
  ceEditComment(postIndex) {
    const result = this.ceResults.find(r => r.post_id === this.cePosts[postIndex].id);
    if (!result) return;

    const textEl = document.getElementById(`ce-comment-text-${postIndex}`);
    if (!textEl) return;

    // Grab the comment box BEFORE outerHTML detaches textEl from DOM
    const commentBox = textEl.closest('.ce-comment-box');
    const currentText = result.comment;
    textEl.outerHTML = `<textarea class="ce-comment-edit" id="ce-comment-edit-${postIndex}" oninput="App.ceEditWc(${postIndex})">${this.esc(currentText)}</textarea>`;

    // Replace edit button with save/cancel using the saved reference
    const actionsEl = commentBox.querySelector('.ce-comment-actions');
    if (actionsEl) {
      const editBtn = actionsEl.querySelector('.ce-edit-btn');
      if (editBtn) {
        editBtn.outerHTML = `
          <button class="ce-edit-btn save" onclick="App.ceSaveComment(${postIndex})">Save</button>
          <button class="ce-edit-btn cancel" onclick="App.ceCancelEdit(${postIndex})">Cancel</button>
        `;
      }
    }

    // Focus the textarea
    const textarea = document.getElementById(`ce-comment-edit-${postIndex}`);
    if (textarea) { textarea.focus(); textarea.setSelectionRange(textarea.value.length, textarea.value.length); }
  },

  ceEditWc(postIndex) {
    const textarea = document.getElementById(`ce-comment-edit-${postIndex}`);
    const wcEl = document.getElementById(`ce-comment-wc-${postIndex}`);
    if (textarea && wcEl) {
      const wc = textarea.value.trim().split(/\s+/).filter(Boolean).length;
      wcEl.textContent = `${wc} words`;
    }
  },

  ceSaveComment(postIndex) {
    const textarea = document.getElementById(`ce-comment-edit-${postIndex}`);
    if (!textarea) return;

    const newText = textarea.value.trim();
    const result = this.ceResults.find(r => r.post_id === this.cePosts[postIndex].id);
    if (!result) return;

    // Update result
    result.comment = newText;
    result.word_count = newText.split(/\s+/).filter(Boolean).length;
    result.source = result.source === 'llm' ? 'llm (edited)' : 'edited';

    // Re-render the comment display
    const commentBox = textarea.closest('.ce-comment-box');
    const statusColor = result.status === 'pass' ? 'var(--green)' : result.status === 'fallback' ? 'var(--red)' : 'var(--orange)';
    const source = `<span class="val-badge val-warn">${result.source}</span>`;

    commentBox.innerHTML = `
      <div class="ce-comment-label">
        <span>Generated Comment (${result.word_count} words)</span>
        <span style="font-size:10px; color:${statusColor}; text-transform:uppercase; font-weight:700;">${result.status}</span>
      </div>
      <div class="ce-comment-text" id="ce-comment-text-${postIndex}">"${this.esc(result.comment)}"</div>
      <div class="ce-comment-wc" id="ce-comment-wc-${postIndex}">${result.word_count} words</div>
      <div class="ce-comment-actions">
        <button class="ce-edit-btn" onclick="App.ceEditComment(${postIndex})">Edit</button>
        ${source}
      </div>
    `;
  },

  ceCancelEdit(postIndex) {
    const result = this.ceResults.find(r => r.post_id === this.cePosts[postIndex].id);
    if (!result) return;

    const textarea = document.getElementById(`ce-comment-edit-${postIndex}`);
    if (!textarea) return;

    const commentBox = textarea.closest('.ce-comment-box');
    const statusColor = result.status === 'pass' ? 'var(--green)' : result.status === 'fallback' ? 'var(--red)' : 'var(--orange)';
    const sourceBadge = result.source ? `<span class="val-badge ${result.source === 'llm' ? 'val-pass' : 'val-fail'}">${result.source}</span>` : '';
    const checks = (result.checks || []).map(c => `<span class="val-badge val-${c.status}">${c.label}</span>`).join('');

    commentBox.innerHTML = `
      <div class="ce-comment-label">
        <span>Generated Comment (${result.word_count} words)</span>
        <span style="font-size:10px; color:${statusColor}; text-transform:uppercase; font-weight:700;">${result.status}</span>
      </div>
      <div class="ce-comment-text" id="ce-comment-text-${postIndex}">"${this.esc(result.comment)}"</div>
      <div class="ce-comment-wc" id="ce-comment-wc-${postIndex}">${result.word_count} words</div>
      <div class="ce-comment-actions">
        <button class="ce-edit-btn" onclick="App.ceEditComment(${postIndex})">Edit</button>
        ${sourceBadge}
      </div>
      ${checks ? `<div class="validation-row">${checks}</div>` : ''}
    `;
  },

  // ─── View Config Modal ──────────────────────────
  async ceViewConfig() {
    const modal = document.getElementById('ceConfigModal');
    const body = document.getElementById('ceConfigModalBody');
    body.innerHTML = '<div style="text-align:center; padding:40px; color:var(--text-3);">Loading config...</div>';
    modal.classList.remove('hidden');

    try {
      const res = await this.authFetch('/api/ce/config/full');
      const data = await res.json();
      if (!data.loaded) {
        body.innerHTML = '<div style="text-align:center; padding:40px; color:var(--text-3);">No config loaded. Upload a config first.</div>';
        return;
      }
      const config = data.config;
      const brand = config.brand || {};
      const templates = config.templates || {};

      let html = '';

      // Brand section
      html += `<div class="cfg-section">
        <div class="cfg-section-title">Brand</div>
        <div class="cfg-row"><span class="cfg-key">Name</span><span class="cfg-val">${this.esc(brand.name || '—')}</span></div>`;
      if (brand.preferred_casing) {
        const casings = Object.entries(brand.preferred_casing).map(([form, w]) => `"${this.esc(form)}" (${Math.round(w*100)}%)`).join(', ');
        html += `<div class="cfg-row"><span class="cfg-key">Preferred Casing</span><span class="cfg-val">${casings}</span></div>`;
      }
      html += `</div>`;

      // Templates
      for (const [slug, tc] of Object.entries(templates)) {
        html += `<div class="cfg-section">
          <div class="cfg-section-title">Template: ${this.esc(slug)}</div>`;

        if (tc.theme_story) html += `<div class="cfg-row"><span class="cfg-key">Theme</span><span class="cfg-val cfg-val-long">${this.esc(tc.theme_story)}</span></div>`;
        if (tc.commenting_persona) html += `<div class="cfg-row"><span class="cfg-key">Persona</span><span class="cfg-val cfg-val-long">${this.esc(tc.commenting_persona)}</span></div>`;

        // Relevance ratio
        if (tc.relevance_ratio !== undefined) {
          html += `<div class="cfg-row"><span class="cfg-key">Relevance Ratio</span><span class="cfg-val">${Math.round(tc.relevance_ratio*100)}% specific / ${Math.round((1-tc.relevance_ratio)*100)}% vibe</span></div>`;
        }

        // Comment rules
        const rules = tc.comment_rules || {};
        if (rules.word_count_range) html += `<div class="cfg-row"><span class="cfg-key">Word Count</span><span class="cfg-val">${rules.word_count_range[0]}–${rules.word_count_range[1]} words</span></div>`;
        if (rules.slang_frequency) html += `<div class="cfg-row"><span class="cfg-key">Slang Frequency</span><span class="cfg-val">${rules.slang_frequency}</span></div>`;
        if (rules.allowed_slang) html += `<div class="cfg-row"><span class="cfg-key">Allowed Slang</span><span class="cfg-val cfg-val-long">${rules.allowed_slang.join(', ')}</span></div>`;

        // Archetype weights
        if (tc.archetype_weights) {
          const weights = Object.entries(tc.archetype_weights)
            .sort((a,b) => b[1]-a[1])
            .map(([arch, w]) => `<span class="cfg-arch-tag arch-${arch}">${(ARCHETYPES[arch]||{}).label||arch} ${Math.round(w*100)}%</span>`).join('');
          html += `<div class="cfg-row"><span class="cfg-key">Archetype Weights</span><div class="cfg-val cfg-arch-tags">${weights}</div></div>`;
        }

        // Golden comments
        const golden = tc.golden_comments || [];
        if (golden.length) {
          html += `<div class="cfg-row"><span class="cfg-key">Golden Comments (${golden.length})</span><div class="cfg-val cfg-list">`;
          golden.forEach(c => { html += `<div class="cfg-golden">"${this.esc(c)}"</div>`; });
          html += `</div></div>`;
        }

        // Anti-examples
        const anti = tc.anti_examples || [];
        if (anti.length) {
          html += `<div class="cfg-row"><span class="cfg-key">Anti-Examples (${anti.length})</span><div class="cfg-val cfg-list">`;
          anti.forEach(a => { html += `<div class="cfg-anti"><span class="cfg-anti-text">"${this.esc(a.text)}"</span><span class="cfg-anti-reason">${this.esc(a.reason)}</span></div>`; });
          html += `</div></div>`;
        }

        // Banned patterns
        const banned = tc.banned_patterns || [];
        if (banned.length) {
          const tags = banned.map(b => `<span class="cfg-banned-tag">${this.esc(b)}</span>`).join('');
          html += `<div class="cfg-row"><span class="cfg-key">Banned Patterns</span><div class="cfg-val cfg-arch-tags">${tags}</div></div>`;
        }

        html += `</div>`;
      }

      body.innerHTML = html;
    } catch (e) {
      body.innerHTML = `<div style="text-align:center; padding:40px; color:var(--red);">Error loading config: ${this.esc(e.message)}</div>`;
    }
  },

  ceCloseConfigModal() {
    document.getElementById('ceConfigModal').classList.add('hidden');
  },

  // ─── Sort / Filter Controls ──────────────────────────
  ceSortBy(mode) {
    this.ceSortMode = mode;
    document.querySelectorAll('.ce-sf-btn[data-sort]').forEach(b => b.classList.toggle('active', b.dataset.sort === mode));
    this.ceReRenderAllResults();
  },

  ceFilterBy(mode) {
    this.ceFilterMode = mode;
    document.querySelectorAll('.ce-sf-btn[data-filter]').forEach(b => b.classList.toggle('active', b.dataset.filter === mode));
    this.ceReRenderAllResults();
  },

  _ceStatusPriority(status) {
    if (status === 'fallback') return 0;
    if (status === 'flagged') return 1;
    return 2; // pass or anything else
  },

  ceReRenderAllResults() {
    if (!this.ceResults.length) return;

    // Build a combined list of post + result pairs with original index
    const resultMap = {};
    this.ceResults.forEach(r => { resultMap[r.post_id] = r; });

    let items = this.cePosts.map((p, i) => ({
      post: p,
      result: resultMap[p.id] || null,
      origIndex: i,
    }));

    // Filter
    if (this.ceFilterMode !== 'all') {
      items = items.filter(item => item.result && item.result.status === this.ceFilterMode);
    }

    // Sort
    if (this.ceSortMode === 'attention') {
      items.sort((a, b) => {
        const pa = a.result ? this._ceStatusPriority(a.result.status) : -1;
        const pb = b.result ? this._ceStatusPriority(b.result.status) : -1;
        return pa - pb;
      });
    } else if (this.ceSortMode === 'account') {
      items.sort((a, b) => (a.post.account_username || '').localeCompare(b.post.account_username || ''));
    }
    // 'default' keeps original order

    // Update count
    const countEl = document.getElementById('ceSfCount');
    if (countEl) countEl.textContent = `${items.length} of ${this.cePosts.length} posts`;

    // Build assignment map
    const assignMap = {};
    (this.ceAssignments || []).forEach(a => { assignMap[a.post_id] = a; });

    // Render
    const container = document.getElementById('cePostsList');
    if (!items.length) {
      container.innerHTML = '<div class="card" style="text-align:center; color:var(--text-3); padding:40px;">No posts match this filter.</div>';
      return;
    }

    let html = '';
    let lastAccount = null;
    items.forEach((item, vi) => {
      const { post: p, result, origIndex: i } = item;
      const assign = assignMap[p.id] || null;
      const arch = assign ? assign.archetype : '';
      const brandMention = assign ? assign.brand_mention : true;
      const relTag = this.ceRelevanceTags[p.id] || '';

      // Account divider for account sort
      if (this.ceSortMode === 'account' && p.account_username !== lastAccount) {
        lastAccount = p.account_username;
        html += `<div class="batch-divider"><div class="batch-divider-line"></div><div class="batch-divider-label">@${this.esc(p.account_username)}</div><div class="batch-divider-line"></div></div>`;
      }

      const slides = (p.slide_texts||[]).map((t,j) => `<div class="slide-text"><span class="slide-num">S${j+1}</span>${this.esc(t)}</div>`).join('');
      const statusLabel = p.status || 'succeeded';
      const statusClass = statusLabel === 'scheduled' ? 'post-status-scheduled' : '';

      html += `
        <div class="ce-post-card fade-in ${result ? 'has-comment' : ''}" id="ce-post-${i}" style="animation-delay:${Math.min(vi*20,300)}ms">
          <div class="ce-post-top">
            <div class="ce-post-info">
              <div class="ce-post-num">POST ${i+1}</div>
              <div class="ce-post-acct">@${this.esc(p.account_username)}</div>
              ${p.template_name ? `<span class="post-template" style="margin-top:4px">${this.esc(p.template_name)}</span>` : ''}
              <span class="post-type ${statusClass}" style="${statusLabel === 'succeeded' ? 'color:var(--green);background:var(--green-dim);' : ''} margin-top:4px">${statusLabel}</span>
            </div>
            ${p.tiktok_url
              ? `<a href="${p.tiktok_url}" target="_blank" class="ce-post-link">${p.tiktok_url}</a>`
              : '<span style="font-size:11px; color:var(--text-3);">No TikTok link</span>'}
          </div>
          ${p.hook ? `<div class="ce-post-hook">${this.esc(p.hook)}</div>` : ''}
          ${slides ? `<div class="ce-post-slides">${slides}</div>` : ''}
          <div class="archetype-row">
            ${arch ? `<span class="archetype-badge arch-${arch}">${(ARCHETYPES[arch]||{}).label||arch}</span>` : ''}
            ${assign ? `<span class="brand-mention-badge ${brandMention ? 'yes' : ''}">${brandMention ? 'Mention brand' : 'No brand mention'}</span>` : ''}
            ${relTag ? `<span class="relevance-badge ${relTag}">${relTag}</span>` : ''}
          </div>`;

      // Comment box
      if (result) {
        const statusColor = result.status === 'pass' ? 'var(--green)' : result.status === 'fallback' ? 'var(--red)' : result.status === 'flagged' ? 'var(--orange)' : 'var(--text-3)';
        const boxClass = result.status === 'pass' ? 'valid' : result.status === 'fallback' ? 'invalid' : result.status === 'flagged' ? 'valid' : 'pending';
        const checks = (result.checks || []).map(c => `<span class="val-badge val-${c.status}">${c.label}</span>`).join('');
        const source = result.source ? `<span class="val-badge ${result.source === 'llm' ? 'val-pass' : 'val-warn'}">${result.source}</span>` : '';

        html += `
          <div class="ce-comment-box ${boxClass}">
            <div class="ce-comment-label">
              <span>Generated Comment (${result.word_count} words)</span>
              <span style="font-size:10px; color:${statusColor}; text-transform:uppercase; font-weight:700;">${result.status}</span>
            </div>
            <div class="ce-comment-text" id="ce-comment-text-${i}">"${this.esc(result.comment)}"</div>
            <div class="ce-comment-wc" id="ce-comment-wc-${i}">${result.word_count} words</div>
            <div class="ce-comment-actions">
              <button class="ce-edit-btn" onclick="App.ceEditComment(${i})">Edit</button>
              ${source}
            </div>
            ${checks ? `<div class="validation-row">${checks}</div>` : ''}
          </div>`;
      } else {
        html += `
          <div class="ce-comment-box pending">
            <div class="ce-comment-label"><span>Generated Comment</span></div>
            <div class="ce-comment-placeholder">No result</div>
          </div>`;
      }

      html += `</div>`;
    });

    container.innerHTML = html;
  },

  // ─── Export Comments ──────────────────────────
  ceExport() {
    if (!this.ceResults.length) return;
    this.ceSetStage('export');

    // Sort by account_username for easy posting workflow
    const sorted = [...this.ceResults].sort((a, b) =>
      (a.account_username || '').localeCompare(b.account_username || '')
    );

    const rows = [['post_index','account_username','tiktok_url','archetype','brand_mention','comment','word_count','source','status']];
    sorted.forEach((r, i) => {
      rows.push([i+1, r.account_username, r.tiktok_url||'', r.archetype, r.brand_mention?'yes':'no', r.comment, r.word_count, r.source, r.status]);
    });
    const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `comments_${this.formatDate(new Date())}.csv`;
    a.click();
    this.ceSetStage('export');
  },

  esc(s) { if (!s) return ''; const d = document.createElement('div'); d.textContent = s; return d.innerHTML; },
};

document.addEventListener('DOMContentLoaded', () => App.init());
