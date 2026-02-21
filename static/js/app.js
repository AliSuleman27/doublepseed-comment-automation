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

  // Comment engine state
  cePosts: [],
  ceResults: [],
  ceAssignments: [],
  ceConfigLoaded: false,
  ceConfigTemplates: [],

  async init() {
    console.log('[DS] init');

    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    document.getElementById('startDate').value = this.formatDate(yesterday);
    document.getElementById('endDate').value = this.formatDate(today);
    document.getElementById('ceDate').value = this.formatDate(yesterday);

    const sidebarItems = document.querySelectorAll('.sb-item[data-pid]');
    if (sidebarItems.length > 0) {
      this.products = Array.from(sidebarItems).map(el => ({
        id: el.dataset.pid,
        title: el.querySelector('span:last-child').textContent.trim()
      }));
      this.setStatus('ready');
    } else {
      await this.loadProducts();
    }

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

    // Check if config already loaded on server
    this.ceCheckConfig();
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
      const res = await fetch('/api/products');
      const data = await res.json();
      if (data.error) { this.showWarning(data.error); this.setStatus('error'); return; }
      if (Array.isArray(data) && data.length > 0) {
        this.products = data;
        this.renderSidebar(data);
        this.renderProductDropdown(data);
        this.setStatus('ready');
      } else {
        const testRes = await fetch('/api/test');
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
      const res = await fetch(`/api/products/${productId}/templates`);
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
      const res = await fetch('/api/posts/fetch', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
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
      const res = await fetch('/api/posts/export', { method: 'POST' });
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
      const res = await fetch('/api/ce/config');
      const data = await res.json();
      if (data.loaded) {
        this.ceConfigLoaded = true;
        this.ceConfigTemplates = data.templates || [];
        document.getElementById('ceConfigStatus').textContent = `${data.brand} — ${data.templates.join(', ')}`;
        document.getElementById('ceConfigStatus').style.color = 'var(--green)';
        document.getElementById('ceConfigInfo').textContent = 'Config loaded on server';
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
      const res = await fetch('/api/ce/config', { method: 'POST', body: formData });
      const data = await res.json();
      if (data.error) { info.textContent = 'Error: ' + data.error; info.style.color = 'var(--red)'; return; }

      this.ceConfigLoaded = true;
      this.ceConfigTemplates = data.templates || [];
      document.getElementById('ceConfigStatus').textContent = `${data.brand} — ${data.templates.join(', ')}`;
      document.getElementById('ceConfigStatus').style.color = 'var(--green)';
      info.textContent = `Loaded: ${data.brand} with ${data.templates.length} template(s)`;
      info.style.color = 'var(--green)';
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
      const res = await fetch(`/api/products/${productId}/templates`);
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

  ceOnTemplateChange() {},

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

      const res = await fetch('/api/posts/fetch', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
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
    const stages = ['select', 'archetype', 'generate', 'validate', 'export'];
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

    let prepData;
    try {
      const prepRes = await fetch('/api/ce/prepare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          posts: this.cePosts,
          template_slug: templateSlug,
          model: model,
          batch_size: batchSize,
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
        const batchRes = await fetch(`/api/ce/batch/${bi}`, {
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
    this.ceSetStage('export');
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

    btn.disabled = false;
    btn.textContent = 'Run Full Pipeline';
    btn.classList.remove('generating');
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
    const wcEl = document.getElementById(`ce-comment-wc-${postIndex}`);
    if (!textEl) return;

    const currentText = result.comment;
    textEl.outerHTML = `<textarea class="ce-comment-edit" id="ce-comment-edit-${postIndex}" oninput="App.ceEditWc(${postIndex})">${this.esc(currentText)}</textarea>`;
    if (wcEl) wcEl.id = `ce-comment-wc-${postIndex}`;

    // Replace edit button with save/cancel
    const actionsEl = textEl.closest('.ce-comment-box').querySelector('.ce-comment-actions');
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

  // ─── Export Comments ──────────────────────────
  ceExport() {
    if (!this.ceResults.length) return;
    const rows = [['post_index','account_username','tiktok_url','archetype','brand_mention','comment','word_count','source','status']];
    this.ceResults.forEach((r, i) => {
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
